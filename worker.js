// VPS监控面板 - Cloudflare Worker解决方案
// 版本: 1.1.0
// ==================== 配置常量 ====================

// 获取管理员账户配置
function getAdminConfig(env) {
  return {
    USERNAME: env.USERNAME || 'admin',
    PASSWORD: env.PASSWORD || 'monitor2025!',
  };
}

// 安全配置 - 增强验证
function getSecurityConfig(env) {
  // 验证关键安全配置
  if (!env.JWT_SECRET || env.JWT_SECRET === 'default-jwt-secret-please-set-in-worker-variables') {
    throw new Error('JWT_SECRET must be set in environment variables for security');
  }

  return {
    JWT_SECRET: env.JWT_SECRET,
    TOKEN_EXPIRY: 2 * 60 * 60 * 1000, // 2小时
    MAX_LOGIN_ATTEMPTS: 5,
    LOGIN_ATTEMPT_WINDOW: 15 * 60 * 1000, // 15分钟
    API_RATE_LIMIT: 60, // 每分钟60次
    MIN_PASSWORD_LENGTH: 8,
    ALLOWED_ORIGINS: env.ALLOWED_ORIGINS ? env.ALLOWED_ORIGINS.split(',').map(o => o.trim()) : [],
  };
}

// ==================== 全局存储 ====================

const rateLimitStore = new Map();
const loginAttemptStore = new Map();

// VPS数据批量处理器
class VpsBatchProcessor {
  constructor() {
    this.batchBuffer = [];
    this.lastBatch = Math.floor(Date.now() / 1000);
    this.maxBatchSize = 100; // 最大批量大小
  }

  // 添加VPS上报数据到批量缓冲区
  addReport(serverId, reportData, batchInterval) {
    this.batchBuffer.push({
      serverId,
      timestamp: reportData.timestamp,
      cpu: JSON.stringify(reportData.cpu),
      memory: JSON.stringify(reportData.memory),
      disk: JSON.stringify(reportData.disk),
      network: JSON.stringify(reportData.network),
      uptime: reportData.uptime
    });

    // 检查是否需要立即刷新（时间到或缓冲区满）
    const now = Math.floor(Date.now() / 1000);
    if (now - this.lastBatch >= batchInterval || this.batchBuffer.length >= this.maxBatchSize) {
      return true; // 需要刷新
    }
    return false;
  }

  // 获取并清空批量数据
  getBatchData() {
    const data = [...this.batchBuffer];
    this.batchBuffer = [];
    this.lastBatch = Math.floor(Date.now() / 1000);
    return data;
  }

  // 检查是否需要定时刷新
  shouldFlush(batchInterval) {
    const now = Math.floor(Date.now() / 1000);
    return this.batchBuffer.length > 0 && (now - this.lastBatch >= batchInterval);
  }
}

// 全局批量处理器实例
const vpsBatchProcessor = new VpsBatchProcessor();

// 批量写入VPS数据到数据库
async function flushVpsBatchData(env) {
  const batchData = vpsBatchProcessor.getBatchData();
  if (batchData.length === 0) return;

  try {
    // 使用D1的batch操作进行批量写入
    const statements = batchData.map(report =>
      env.DB.prepare(`
        REPLACE INTO metrics (server_id, timestamp, cpu, memory, disk, network, uptime)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `).bind(
        report.serverId,
        report.timestamp,
        report.cpu,
        report.memory,
        report.disk,
        report.network,
        report.uptime
      )
    );

    await env.DB.batch(statements);
    console.log(`批量写入${batchData.length}条VPS数据`);
  } catch (error) {
    console.error('批量写入VPS数据失败:', error);
    // 如果批量写入失败，将数据重新加入缓冲区
    vpsBatchProcessor.batchBuffer.unshift(...batchData);
    throw error;
  }
}

// 定时刷新VPS批量数据（在主请求处理中调用）
async function scheduleVpsBatchFlush(env, ctx) {
  try {
    const batchInterval = await getVpsReportInterval(env);
    if (vpsBatchProcessor.shouldFlush(batchInterval)) {
      ctx.waitUntil(flushVpsBatchData(env));
    }
  } catch (error) {
    // 使用默认间隔60秒
    if (vpsBatchProcessor.shouldFlush(60)) {
      ctx.waitUntil(flushVpsBatchData(env));
    }
  }
}

// ==================== 监控状态阈值常量 ====================
// HIGH_LATENCY 阈值：成功响应但响应过慢时单独标记，仅展示不触发故障通知
const WEBSITE_HIGH_LATENCY_MS = 3000;       // 网站HEAD检查 >3s 视为高延迟
const WEBSITE_CHECK_TIMEOUT_MS = 15000;     // 网站检查超时时间（原10s，提高以容纳高延迟判定）
const LLM_HIGH_LATENCY_MS = 20000;          // LLM POST+推理 >20s 视为高延迟
const DEFAULT_LLM_TIMEOUT_MS = 45000;       // LLM 默认超时
const MIN_LLM_TIMEOUT_MS = 21000;           // LLM 默认超时下限，实际下限需高于高延迟阈值
const MIN_LLM_TIMEOUT_BUFFER_MS = 1000;     // LLM 超时需至少比高延迟阈值多 1 秒
const MAX_LLM_TIMEOUT_MS = 120000;          // LLM 超时上限

function parsePositiveInteger(value, fallback) {
  const parsed = parseInt(value, 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function getEffectiveMinLlmTimeout(llmHighLatencyMs = LLM_HIGH_LATENCY_MS) {
  const threshold = parsePositiveInteger(llmHighLatencyMs, LLM_HIGH_LATENCY_MS);
  return Math.max(MIN_LLM_TIMEOUT_MS, threshold + MIN_LLM_TIMEOUT_BUFFER_MS);
}

// 规范化 LLM 超时：缺省/越界回退到默认值或当前有效下限
function normalizeLlmTimeout(timeout_ms, llmHighLatencyMs = LLM_HIGH_LATENCY_MS) {
  const v = Number(timeout_ms);
  const minTimeoutMs = getEffectiveMinLlmTimeout(llmHighLatencyMs);
  const fallbackTimeout = Math.min(MAX_LLM_TIMEOUT_MS, Math.max(DEFAULT_LLM_TIMEOUT_MS, minTimeoutMs));
  if (!Number.isFinite(v) || v < minTimeoutMs || v > MAX_LLM_TIMEOUT_MS) {
    return fallbackTimeout;
  }
  return Math.floor(v);
}

// ==================== 配置缓存系统 ====================

class ConfigCache {
  constructor() {
    this.cache = new Map();
    this.CACHE_TTL = {
      TELEGRAM: 5 * 60 * 1000,    // 5分钟
      MONITORING: 5 * 60 * 1000,  // 5分钟
      SERVERS: 2 * 60 * 1000      // 2分钟
    };
  }

  set(key, value, ttl) {
    this.cache.set(key, {
      value,
      timestamp: Date.now(),
      ttl
    });
  }

  get(key) {
    const entry = this.cache.get(key);
    if (!entry) return null;

    if (Date.now() - entry.timestamp > entry.ttl) {
      this.cache.delete(key);
      return null;
    }

    return entry.value;
  }

  async getTelegramConfig(db) {
    const cached = this.get('telegram_config');
    if (cached) return cached;

    const config = await db.prepare(
      'SELECT bot_token, chat_id, enable_notifications FROM telegram_config WHERE id = 1'
    ).first();

    if (config) {
      this.set('telegram_config', config, this.CACHE_TTL.TELEGRAM);
    }

    return config;
  }

  async getNtfyConfig(db) {
    const cached = this.get('ntfy_config');
    if (cached) return cached;

    const config = await db.prepare(
      'SELECT topic, server_url, enable_notifications FROM ntfy_config WHERE id = 1'
    ).first();

    if (config) {
      this.set('ntfy_config', config, this.CACHE_TTL.TELEGRAM);
    }

    return config;
  }

  async getMonitoringSettings(db) {
    const cached = this.get('monitoring_settings');
    if (cached) return cached;

    const settings = await db.prepare(
      'SELECT key, value FROM app_config WHERE key IN ("vps_report_interval_seconds", "llm_check_interval", "website_high_latency_ms", "llm_high_latency_ms")'
    ).all();

    if (settings?.results) {
      this.set('monitoring_settings', settings.results, this.CACHE_TTL.MONITORING);
      return settings.results;
    }

    return [];
  }

  async getServerList(db, isAdmin = false) {
    const cacheKey = isAdmin ? 'servers_admin' : 'servers_public';
    const cached = this.get(cacheKey);
    if (cached) return cached;

    let query = 'SELECT id, name, description FROM servers';
    if (!isAdmin) {
      query += ' WHERE is_public = 1';
    }
    query += ' ORDER BY sort_order ASC NULLS LAST, name ASC';

    const { results } = await db.prepare(query).all();
    const servers = results || [];

    this.set(cacheKey, servers, this.CACHE_TTL.SERVERS);
    return servers;
  }

  clear() {
    this.cache.clear();
  }

  clearKey(key) {
    this.cache.delete(key);
  }
}

// 全局配置缓存实例
const configCache = new ConfigCache();

// ==================== 定时任务优化 ====================

// 任务执行计数器
let taskCounter = 0;
let dbInitialized = false;

// ==================== 工具函数 ====================

// SQL安全验证 - 防止注入攻击
function validateSqlIdentifier(value, type) {
  const whitelist = {
    column: [
      // common
      'id', 'name', 'url', 'description', 'sort_order', 'is_public', 'timestamp',
      // servers
      'api_key', 'created_at', 'last_notified_down_at',
      // metrics
      'server_id', 'cpu', 'memory', 'disk', 'network', 'uptime',
      // monitored_sites
      'added_at', 'last_checked', 'last_status', 'last_status_code', 'last_response_time_ms',
      // site_status_history / llm_status_history
      'site_id', 'endpoint_id', 'status', 'status_code', 'response_time_ms', 'output_preview',
      // monitored_llm_endpoints
      'api_url', 'model', 'check_prompt', 'expected_contains', 'timeout_ms', 'last_output_preview',
      // admin_credentials
      'password_hash', 'last_login', 'failed_attempts', 'locked_until', 'must_change_password', 'password_changed_at',
      // telegram_config / ntfy_config
      'bot_token', 'chat_id', 'enable_notifications', 'updated_at', 'topic', 'server_url',
      // app_config
      'key', 'value'
    ],
    table: ['servers', 'monitored_sites', 'metrics', 'site_status_history', 'monitored_llm_endpoints', 'llm_status_history', 'admin_credentials', 'telegram_config', 'ntfy_config', 'app_config'],
    order: ['ASC', 'DESC']
  };

  const allowed = whitelist[type];
  if (!allowed || !allowed.includes(value)) {
    throw new Error(`Invalid ${type}: ${value}`);
  }
  return value;
}

// 敏感信息脱敏
function maskSensitive(value, type = 'key') {
  if (!value || typeof value !== 'string') return value;
  return type === 'key' && value.length > 8 ? value.substring(0, 8) + '***' : '***';
}

// 增强的令牌撤销机制 - 修复JWT缓存安全问题
const revokedTokens = new Map(); // 改为Map存储撤销时间

function revokeToken(token) {
  revokedTokens.set(token, Date.now());
  // 清理JWT缓存中的对应令牌
  jwtCache.delete(token);

  // 定期清理过期的撤销记录（24小时后清理）
  if (Math.random() < 0.01) {
    const expireTime = Date.now() - 24 * 60 * 60 * 1000;
    for (const [revokedToken, revokeTime] of revokedTokens.entries()) {
      if (revokeTime < expireTime) {
        revokedTokens.delete(revokedToken);
      }
    }
  }
}

function isTokenRevoked(token) {
  return revokedTokens.has(token);
}

// 安全的JSON解析 - 限制大小
async function parseJsonSafely(request, maxSize = 1024 * 1024) {
  const contentLength = request.headers.get('content-length');
  if (contentLength && parseInt(contentLength) > maxSize) {
    throw new Error('Request body too large');
  }

  const text = await request.text();
  if (text.length > maxSize) {
    throw new Error('Request body too large');
  }

  return JSON.parse(text);
}

// 增强的管理员认证 - 修复权限检查问题
async function authenticateAdmin(request, env) {
  const user = await authenticateRequest(request, env);
  if (!user) return null;

  // 验证用户确实存在于管理员表中且未被锁定
  const adminUser = await env.DB.prepare(
    'SELECT username, locked_until FROM admin_credentials WHERE username = ?'
  ).bind(user.username).first();

  if (!adminUser || (adminUser.locked_until && Date.now() < adminUser.locked_until)) {
    return null;
  }

  return user;
}

// 严格的管理员权限检查装饰器
function requireAdmin(handler) {
  return async (request, env, corsHeaders, ...args) => {
    const user = await authenticateAdmin(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }
    return handler(request, env, corsHeaders, user, ...args);
  };
}

// 路径参数验证
function extractPathSegment(path, index) {
  const segments = path.split('/');

  // 支持负数索引（从末尾开始）
  if (index < 0) {
    index = segments.length + index;
  }

  if (index < 0 || index >= segments.length) return null;

  const segment = segments[index];
  return segment && /^[a-zA-Z0-9_-]{1,50}$/.test(segment) ? segment : null;
}

// 提取服务器ID的便捷函数
function extractAndValidateServerId(path) {
  return extractPathSegment(path, -1);
}

// 增强的输入验证 - 修复SSRF漏洞
function validateInput(input, type, maxLength = 255) {
  if (!input || typeof input !== 'string' || input.length > maxLength) {
    return false;
  }

  const cleaned = input.trim();

  const validators = {
    serverName: () => {
      if (!/^[\w\s\u4e00-\u9fa5.-]{2,50}$/.test(cleaned)) return false;
      const sqlKeywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'SCRIPT', 'UNION', 'OR', 'AND'];
      return !sqlKeywords.some(keyword => cleaned.toUpperCase().includes(keyword));
    },
    description: () => {
      if (cleaned.length > 500) return false;
      return !/<[^>]*>|javascript:|on\w+\s*=|<script/i.test(cleaned);
    },
    direction: () => ['up', 'down'].includes(input),
    url: () => {
      try {
        const url = new URL(input);
        if (!['http:', 'https:'].includes(url.protocol)) return false;

        // 增强的内网地址检查 - 修复SSRF
        const hostname = url.hostname.toLowerCase();

        // IPv4内网检查
        if (hostname === 'localhost' || hostname === '0.0.0.0' ||
            hostname.startsWith('127.') || hostname.startsWith('10.') ||
            hostname.startsWith('192.168.') || hostname.startsWith('169.254.') ||
            (hostname.startsWith('172.') &&
             parseInt(hostname.split('.')[1]) >= 16 &&
             parseInt(hostname.split('.')[1]) <= 31)) {
          return false;
        }

        // IPv6内网检查 - 修复方括号处理
        if (hostname.includes(':')) {
          // 移除方括号（如果存在）
          const cleanHostname = hostname.replace(/^\[|\]$/g, '');
          if (cleanHostname === '::1' || cleanHostname.startsWith('fc') ||
              cleanHostname.startsWith('fd') || cleanHostname.startsWith('fe80')) {
            return false;
          }
        }

        // 域名黑名单检查
        const blockedDomains = ['internal', 'local', 'intranet', 'corp'];
        if (blockedDomains.some(domain => hostname.includes(domain))) {
          return false;
        }

        // 端口限制 - 只允许标准HTTP/HTTPS端口
        const port = url.port;
        if (port && !['80', '443', '8080', '8443'].includes(port)) {
          return false;
        }

        return input.length <= 2048;
      } catch {
        return false;
      }
    }
  };

  return validators[type] ? validators[type]() : cleaned.length > 0;
}

// ==================== 统一响应处理工具 ====================

// 创建标准API响应
function createApiResponse(data, status = 200, corsHeaders = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...corsHeaders }
  });
}

// 创建错误响应
function createErrorResponse(error, message, status = 500, corsHeaders = {}, details = null) {
  const errorData = {
    error,
    message,
    timestamp: Date.now()
  };
  if (details) errorData.details = details;

  return createApiResponse(errorData, status, corsHeaders);
}

// 创建成功响应
function createSuccessResponse(data, corsHeaders = {}) {
  return createApiResponse({ success: true, ...data }, 200, corsHeaders);
}

// ==================== 统一验证工具 ====================

// 获取Telegram配置（已移至ConfigCache类）

// 服务器认证验证
async function validateServerAuth(path, request, env) {
  const serverId = extractAndValidateServerId(path);
  if (!serverId) {
    return { error: 'Invalid server ID', message: '无效的服务器ID格式' };
  }

  const apiKey = request.headers.get('X-API-Key');
  if (!apiKey) {
    return { error: 'API key required', message: '需要API密钥' };
  }

  try {
    const serverData = await env.DB.prepare(
      'SELECT id, name, api_key FROM servers WHERE id = ?'
    ).bind(serverId).first();

    if (!serverData || serverData.api_key !== apiKey) {
      return { error: 'Invalid credentials', message: '无效的服务器ID或API密钥' };
    }

    return { success: true, serverId, serverData };
  } catch (error) {
    return { error: 'Database error', message: '数据库查询失败' };
  }
}

// ==================== 统一数据库错误处理 ====================

function handleDbError(error, corsHeaders, operation = 'database operation') {
  if (error.message.includes('no such table')) {
    return createErrorResponse(
      'Database table missing',
      '数据库表不存在，请重试',
      503,
      corsHeaders
    );
  }

  return createErrorResponse(
    'Internal server error',
    '系统暂时不可用，请稍后重试',
    500,
    corsHeaders
  );
}

// ==================== 缓存查询工具 ====================

// VPS上报间隔缓存
let vpsIntervalCache = {
  value: null,
  timestamp: 0,
  ttl: 60000 // 1分钟缓存
};

// 获取VPS上报间隔（带缓存）
async function getVpsReportInterval(env) {
  const now = Date.now();

  // 检查缓存是否有效
  if (vpsIntervalCache.value !== null && (now - vpsIntervalCache.timestamp) < vpsIntervalCache.ttl) {
    return vpsIntervalCache.value;
  }

  try {
    const result = await env.DB.prepare(
      'SELECT value FROM app_config WHERE key = ?'
    ).bind('vps_report_interval_seconds').first();

    const interval = result?.value ? parseInt(result.value, 10) : 60;
    if (!isNaN(interval) && interval > 0) {
      // 更新缓存
      vpsIntervalCache.value = interval;
      vpsIntervalCache.timestamp = now;
      return interval;
    }
  } catch (error) {
    // 静默处理错误，使用默认值
  }

  // 默认值也缓存
  vpsIntervalCache.value = 60;
  vpsIntervalCache.timestamp = now;
  return 60;
}

async function getMonitoringThresholds(db) {
  try {
    const settings = await configCache.getMonitoringSettings(db);
    const settingsMap = new Map((settings || []).map(item => [item.key, item.value]));

    return {
      websiteHighLatencyMs: parsePositiveInteger(settingsMap.get('website_high_latency_ms'), WEBSITE_HIGH_LATENCY_MS),
      llmHighLatencyMs: parsePositiveInteger(settingsMap.get('llm_high_latency_ms'), LLM_HIGH_LATENCY_MS)
    };
  } catch (error) {
    return {
      websiteHighLatencyMs: WEBSITE_HIGH_LATENCY_MS,
      llmHighLatencyMs: LLM_HIGH_LATENCY_MS
    };
  }
}

async function getExistingProviderName(db, apiUrl) {
  if (!apiUrl) return '';
  try {
    const row = await db.prepare(
      `SELECT name FROM monitored_llm_endpoints
       WHERE api_url = ? AND name IS NOT NULL AND TRIM(name) != ''
       ORDER BY sort_order ASC NULLS LAST, added_at ASC
       LIMIT 1`
    ).bind(apiUrl).first();
    return row?.name ? row.name.trim() : '';
  } catch (error) {
    return '';
  }
}

function clearMonitoringSettingsCache() {
  configCache.clearKey('monitoring_settings');
}

// 清除VPS间隔缓存（当设置更新时调用）
function clearVpsIntervalCache() {
  vpsIntervalCache.value = null;
  vpsIntervalCache.timestamp = 0;
}

// ==================== VPS数据验证工具 ====================

// VPS数据默认值配置
const VPS_DATA_DEFAULTS = {
  cpu: { usage_percent: 0, load_avg: [0, 0, 0] },
  memory: { total: 0, used: 0, free: 0, usage_percent: 0 },
  disk: { total: 0, used: 0, free: 0, usage_percent: 0 },
  network: { upload_speed: 0, download_speed: 0, total_upload: 0, total_download: 0 }
};

// 简化的VPS数据验证和转换
function validateAndFixVpsField(data, field) {
  if (!data || typeof data !== 'object') return VPS_DATA_DEFAULTS[field];

  // 转换字符串数字为数字
  const converted = {};
  for (const [key, value] of Object.entries(data)) {
    converted[key] = typeof value === 'string' ? (parseFloat(value) || 0) : (value || 0);
  }

  return converted;
}

// 简化的VPS数据验证
function validateAndFixVpsData(reportData) {
  const requiredFields = ['timestamp', 'cpu', 'memory', 'disk', 'network', 'uptime'];

  // 检查必需字段
  for (const field of requiredFields) {
    if (!reportData[field]) {
      return { error: 'Invalid data format', message: `缺少字段: ${field}` };
    }
  }

  // 修复数据类型
  ['cpu', 'memory', 'disk', 'network'].forEach(field => {
    reportData[field] = validateAndFixVpsField(reportData[field], field);
  });

  // 修复时间戳和uptime
  reportData.timestamp = parseInt(reportData.timestamp) || Math.floor(Date.now() / 1000);
  reportData.uptime = parseInt(reportData.uptime) || 0;

  return { success: true, data: reportData };
}

// ==================== 密码处理 ====================

async function hashPassword(password) {
  // 生成16字节随机盐值
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const saltHex = Array.from(salt).map(b => b.toString(16).padStart(2, '0')).join('');

  // 组合密码和盐值，进行1000次迭代（平衡安全性和性能）
  const encoder = new TextEncoder();
  let hash = encoder.encode(password + saltHex);

  for (let i = 0; i < 1000; i++) {
    hash = new Uint8Array(await crypto.subtle.digest('SHA-256', hash));
  }

  const hashHex = Array.from(hash).map(b => b.toString(16).padStart(2, '0')).join('');
  return `${saltHex}$${hashHex}`;
}

async function verifyPassword(password, hashedPassword) {
  // 兼容新旧哈希格式
  if (hashedPassword.includes('$')) {
    // 新格式：salt$hash
    const [saltHex, expectedHash] = hashedPassword.split('$');

    const encoder = new TextEncoder();
    let hash = encoder.encode(password + saltHex);

    for (let i = 0; i < 1000; i++) {
      hash = new Uint8Array(await crypto.subtle.digest('SHA-256', hash));
    }

    const computedHash = Array.from(hash).map(b => b.toString(16).padStart(2, '0')).join('');
    return computedHash === expectedHash;
  } else {
    // 旧格式：纯SHA-256（向后兼容）
    const encoder = new TextEncoder();
    const data = encoder.encode(password);
    const hashBuffer = await crypto.subtle.digest('SHA-256', data);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const computedHash = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
    return computedHash === hashedPassword;
  }
}

// ==================== JWT处理 ====================

// JWT验证缓存
const jwtCache = new Map();
const JWT_CACHE_TTL = 60000; // 1分钟缓存
const MAX_CACHE_SIZE = 1000; // 最大缓存条目数

// 清理过期的缓存条目
function cleanupJWTCache() {
  const now = Date.now();
  for (const [key, value] of jwtCache.entries()) {
    if (now - value.timestamp > JWT_CACHE_TTL) {
      jwtCache.delete(key);
    }
  }

  // 如果缓存过大，删除最旧的条目
  if (jwtCache.size > MAX_CACHE_SIZE) {
    const entries = Array.from(jwtCache.entries());
    entries.sort((a, b) => a[1].timestamp - b[1].timestamp);
    const toDelete = entries.slice(0, jwtCache.size - MAX_CACHE_SIZE);
    toDelete.forEach(([key]) => jwtCache.delete(key));
  }
}

async function createJWT(payload, env) {
  const config = getSecurityConfig(env);
  const header = { alg: 'HS256', typ: 'JWT' };
  const now = Date.now();
  const jwtPayload = { ...payload, iat: now, exp: now + config.TOKEN_EXPIRY };

  const encodedHeader = btoa(JSON.stringify(header));
  const encodedPayload = btoa(JSON.stringify(jwtPayload));
  const data = encodedHeader + '.' + encodedPayload;

  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(config.JWT_SECRET),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );

  const signature = await crypto.subtle.sign('HMAC', key, encoder.encode(data));
  const encodedSignature = btoa(String.fromCharCode(...new Uint8Array(signature)));

  return data + '.' + encodedSignature;
}

// 安全的JWT验证函数 - 修复缓存安全问题
async function verifyJWTCached(token, env) {
  // 首先检查令牌是否被撤销
  if (isTokenRevoked(token)) {
    jwtCache.delete(token);
    return null;
  }

  // 检查缓存
  const cached = jwtCache.get(token);
  if (cached && Date.now() - cached.timestamp < JWT_CACHE_TTL) {
    // 检查token是否过期
    if (cached.payload.exp && Date.now() > cached.payload.exp) {
      jwtCache.delete(token);
      return null;
    }
    // 再次检查撤销状态（防止缓存期间被撤销）
    if (isTokenRevoked(token)) {
      jwtCache.delete(token);
      return null;
    }
    return cached.payload;
  }

  // 缓存未命中，执行实际验证
  const payload = await verifyJWT(token, env);
  if (payload && !isTokenRevoked(token)) {
    // 定期清理缓存
    if (Math.random() < 0.01) {
      cleanupJWTCache();
    }

    // 存入缓存
    jwtCache.set(token, {
      payload,
      timestamp: Date.now()
    });
  }

  return payload;
}

// 原始JWT验证函数（不使用缓存）
async function verifyJWT(token, env) {
  try {
    // 检查令牌是否被撤销
    if (isTokenRevoked(token)) return null;

    const config = getSecurityConfig(env);
    const [encodedHeader, encodedPayload, encodedSignature] = token.split('.');
    if (!encodedHeader || !encodedPayload || !encodedSignature) return null;

    const data = encodedHeader + '.' + encodedPayload;
    const encoder = new TextEncoder();
    const key = await crypto.subtle.importKey(
      'raw',
      encoder.encode(config.JWT_SECRET),
      { name: 'HMAC', hash: 'SHA-256' },
      false,
      ['verify']
    );

    const signature = Uint8Array.from(atob(encodedSignature), c => c.charCodeAt(0));
    const isValid = await crypto.subtle.verify('HMAC', key, signature, encoder.encode(data));
    if (!isValid) return null;

    const payload = JSON.parse(atob(encodedPayload));
    if (payload.exp && Date.now() > payload.exp) return null;

    // 检查是否需要刷新令牌
    const tokenAge = Date.now() - payload.iat;
    const halfLife = config.TOKEN_EXPIRY / 2;
    if (tokenAge > halfLife) {
      payload.shouldRefresh = true;
    }

    return payload;
  } catch (error) {
    return null;
  }
}

// ==================== 安全限制 ====================

function checkRateLimit(clientIP, endpoint, env) {
  const config = getSecurityConfig(env);
  const key = `${clientIP}:${endpoint}`;
  const now = Date.now();
  const windowStart = now - 60000;

  if (!rateLimitStore.has(key)) {
    rateLimitStore.set(key, []);
  }

  const requests = rateLimitStore.get(key);
  const validRequests = requests.filter(timestamp => timestamp > windowStart);

  if (validRequests.length >= config.API_RATE_LIMIT) {
    return false;
  }

  validRequests.push(now);
  rateLimitStore.set(key, validRequests);
  return true;
}

function checkLoginAttempts(clientIP, env) {
  const config = getSecurityConfig(env);
  const now = Date.now();
  const windowStart = now - config.LOGIN_ATTEMPT_WINDOW;

  if (!loginAttemptStore.has(clientIP)) {
    loginAttemptStore.set(clientIP, []);
  }

  const attempts = loginAttemptStore.get(clientIP);
  const validAttempts = attempts.filter(timestamp => timestamp > windowStart);
  return validAttempts.length < config.MAX_LOGIN_ATTEMPTS;
}

function recordLoginAttempt(clientIP) {
  const now = Date.now();
  if (!loginAttemptStore.has(clientIP)) {
    loginAttemptStore.set(clientIP, []);
  }
  loginAttemptStore.get(clientIP).push(now);
}

function getClientIP(request) {
  return request.headers.get('CF-Connecting-IP') ||
         request.headers.get('X-Forwarded-For') ||
         request.headers.get('X-Real-IP') ||
         '127.0.0.1';
}

// ==================== 数据库结构 ====================

const D1_SCHEMAS = {
  admin_credentials: `
    CREATE TABLE IF NOT EXISTS admin_credentials (
      username TEXT PRIMARY KEY,
      password_hash TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      last_login INTEGER,
      failed_attempts INTEGER DEFAULT 0,
      locked_until INTEGER DEFAULT NULL,
      must_change_password INTEGER DEFAULT 0,
      password_changed_at INTEGER DEFAULT NULL
    );`,

  servers: `
    CREATE TABLE IF NOT EXISTS servers (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT,
      api_key TEXT NOT NULL UNIQUE,
      created_at INTEGER NOT NULL,
      sort_order INTEGER,
      last_notified_down_at INTEGER DEFAULT NULL,
      is_public INTEGER DEFAULT 1
    );`,

  metrics: `
    CREATE TABLE IF NOT EXISTS metrics (
      server_id TEXT PRIMARY KEY,
      timestamp INTEGER,
      cpu TEXT,
      memory TEXT,
      disk TEXT,
      network TEXT,
      uptime INTEGER,
      FOREIGN KEY(server_id) REFERENCES servers(id) ON DELETE CASCADE
    );`,

  monitored_sites: `
    CREATE TABLE IF NOT EXISTS monitored_sites (
      id TEXT PRIMARY KEY,
      url TEXT NOT NULL UNIQUE,
      name TEXT,
      added_at INTEGER NOT NULL,
      last_checked INTEGER,
      last_status TEXT DEFAULT 'PENDING',
      last_status_code INTEGER,
      last_response_time_ms INTEGER,
      sort_order INTEGER,
      last_notified_down_at INTEGER DEFAULT NULL,
      is_public INTEGER DEFAULT 1
    );`,

  site_status_history: `
    CREATE TABLE IF NOT EXISTS site_status_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      site_id TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      status TEXT NOT NULL,
      status_code INTEGER,
      response_time_ms INTEGER,
      FOREIGN KEY(site_id) REFERENCES monitored_sites(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_site_status_history_site_id_timestamp ON site_status_history (site_id, timestamp DESC);`,

  telegram_config: `
    CREATE TABLE IF NOT EXISTS telegram_config (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      bot_token TEXT,
      chat_id TEXT,
      enable_notifications INTEGER DEFAULT 0,
      updated_at INTEGER
    );
    INSERT OR IGNORE INTO telegram_config (id, bot_token, chat_id, enable_notifications, updated_at) VALUES (1, NULL, NULL, 0, NULL);`,

  ntfy_config: `
    CREATE TABLE IF NOT EXISTS ntfy_config (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      topic TEXT,
      server_url TEXT DEFAULT 'https://ntfy.sh',
      enable_notifications INTEGER DEFAULT 0,
      updated_at INTEGER
    );
    INSERT OR IGNORE INTO ntfy_config (id, topic, server_url, enable_notifications, updated_at) VALUES (1, NULL, 'https://ntfy.sh', 0, NULL);`,

  app_config: `
    CREATE TABLE IF NOT EXISTS app_config (
      key TEXT PRIMARY KEY,
      value TEXT
    );
    INSERT OR IGNORE INTO app_config (key, value) VALUES ('vps_report_interval_seconds', '60');
    INSERT OR IGNORE INTO app_config (key, value) VALUES ('custom_background_enabled', 'false');
    INSERT OR IGNORE INTO app_config (key, value) VALUES ('custom_background_url', '');
    INSERT OR IGNORE INTO app_config (key, value) VALUES ('page_opacity', '80');
    INSERT OR IGNORE INTO app_config (key, value) VALUES ('llm_check_interval', '1');
    INSERT OR IGNORE INTO app_config (key, value) VALUES ('website_high_latency_ms', '3000');
    INSERT OR IGNORE INTO app_config (key, value) VALUES ('llm_high_latency_ms', '20000');`,

  monitored_llm_endpoints: `
    CREATE TABLE IF NOT EXISTS monitored_llm_endpoints (
      id TEXT PRIMARY KEY,
      name TEXT,
      api_url TEXT NOT NULL,
      api_key TEXT,
      model TEXT NOT NULL,
      check_prompt TEXT DEFAULT 'Say hello in one word',
      expected_contains TEXT,
      timeout_ms INTEGER DEFAULT 45000,
      added_at INTEGER NOT NULL,
      last_checked INTEGER,
      last_status TEXT DEFAULT 'PENDING',
      last_status_code INTEGER,
      last_response_time_ms INTEGER,
      last_output_preview TEXT,
      sort_order INTEGER,
      last_notified_down_at INTEGER DEFAULT NULL,
      is_public INTEGER DEFAULT 1,
      enable_notifications INTEGER DEFAULT 1
    );`,

  llm_status_history: `
    CREATE TABLE IF NOT EXISTS llm_status_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      endpoint_id TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      status TEXT NOT NULL,
      status_code INTEGER,
      response_time_ms INTEGER,
      output_preview TEXT,
      FOREIGN KEY(endpoint_id) REFERENCES monitored_llm_endpoints(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_llm_status_history_endpoint_id_timestamp ON llm_status_history (endpoint_id, timestamp DESC);`
};

// ==================== 数据库初始化 ====================

async function ensureTablesExist(db, env) {
  try {
    const createTableStatements = Object.values(D1_SCHEMAS).map(sql => db.prepare(sql));
    await db.batch(createTableStatements);
  } catch (error) {
    // 静默处理数据库创建错误
  }

  await createDefaultAdmin(db, env);
  await applySchemaAlterations(db);
}

async function applySchemaAlterations(db) {
  const alterStatements = [
    "ALTER TABLE monitored_sites ADD COLUMN last_notified_down_at INTEGER DEFAULT NULL",
    "ALTER TABLE servers ADD COLUMN last_notified_down_at INTEGER DEFAULT NULL",
    "ALTER TABLE metrics ADD COLUMN uptime INTEGER DEFAULT NULL",
    "ALTER TABLE admin_credentials ADD COLUMN password_hash TEXT",
    "ALTER TABLE admin_credentials ADD COLUMN created_at INTEGER",
    "ALTER TABLE admin_credentials ADD COLUMN last_login INTEGER",
    "ALTER TABLE admin_credentials ADD COLUMN failed_attempts INTEGER DEFAULT 0",
    "ALTER TABLE admin_credentials ADD COLUMN locked_until INTEGER DEFAULT NULL",
    "ALTER TABLE admin_credentials ADD COLUMN must_change_password INTEGER DEFAULT 0",
    "ALTER TABLE admin_credentials ADD COLUMN password_changed_at INTEGER DEFAULT NULL",
    "ALTER TABLE servers ADD COLUMN is_public INTEGER DEFAULT 1",
    "ALTER TABLE monitored_sites ADD COLUMN is_public INTEGER DEFAULT 1",
    "ALTER TABLE monitored_llm_endpoints ADD COLUMN enable_notifications INTEGER DEFAULT 1"
  ];

  for (const alterSql of alterStatements) {
    try {
      await db.exec(alterSql);
    } catch (e) {
      // 静默处理重复列错误
    }
  }

  // 迁移老数据：把 timeout_ms 为 NULL 或旧默认 30000 的 LLM 端点升到 DEFAULT_LLM_TIMEOUT_MS
  // 30000 低于新的 MIN_LLM_TIMEOUT_MS(21000)，会让 HIGH_LATENCY 阈值失效，需要升级
  try {
    await db.prepare(
      'UPDATE monitored_llm_endpoints SET timeout_ms = ? WHERE timeout_ms IS NULL OR timeout_ms = 30000'
    ).bind(DEFAULT_LLM_TIMEOUT_MS).run();
  } catch (e) {
    // 静默处理（表可能尚未创建）
  }

  try {
    await db.batch([
      db.prepare('INSERT OR IGNORE INTO app_config (key, value) VALUES (?, ?)').bind('website_high_latency_ms', String(WEBSITE_HIGH_LATENCY_MS)),
      db.prepare('INSERT OR IGNORE INTO app_config (key, value) VALUES (?, ?)').bind('llm_high_latency_ms', String(LLM_HIGH_LATENCY_MS))
    ]);
  } catch (e) {
    // 静默处理（app_config 表可能尚未创建）
  }
}

async function isUsingDefaultPassword(username, password, env) {
  const adminConfig = getAdminConfig(env);
  return username === adminConfig.USERNAME && password === adminConfig.PASSWORD;
}

async function createDefaultAdmin(db, env) {
  try {
    const adminConfig = getAdminConfig(env);
    const adminExists = await db.prepare(
      "SELECT username FROM admin_credentials WHERE username = ?"
    ).bind(adminConfig.USERNAME).first();

    if (!adminExists) {
      const adminPasswordHash = await hashPassword(adminConfig.PASSWORD);
      const now = Math.floor(Date.now() / 1000);

      await db.prepare(`
        INSERT INTO admin_credentials (username, password_hash, created_at, failed_attempts, must_change_password)
        VALUES (?, ?, ?, 0, 0)
      `).bind(adminConfig.USERNAME, adminPasswordHash, now).run();
    }
  } catch (error) {
    if (!error.message.includes('no such table')) {
      throw error;
    }
  }
}


// ==================== 身份验证 ====================

// 优化的认证函数，使用JWT缓存和智能数据库查询
async function authenticateRequest(request, env) {
  const authHeader = request.headers.get('Authorization');
  if (!authHeader?.startsWith('Bearer ')) return null;

  const token = authHeader.substring(7);
  const payload = await verifyJWTCached(token, env);
  if (!payload) return null;

  // 只有在token需要刷新时才查询数据库验证用户状态
  // 这大大减少了数据库查询次数
  if (payload.shouldRefresh) {
    const user = await env.DB.prepare(
      'SELECT username, locked_until FROM admin_credentials WHERE username = ?'
    ).bind(payload.username).first();

    if (!user || (user.locked_until && Date.now() < user.locked_until)) {
      return null;
    }
  }

  return payload;
}

// 可选认证函数 - 用于前台API，支持游客和管理员两种模式
async function authenticateRequestOptional(request, env) {
  try {
    return await authenticateRequest(request, env);
  } catch (error) {
    return null; // 未登录或认证失败返回null
  }
}

// ==================== CORS处理 ====================

function getSecureCorsHeaders(origin, env) {
  const config = getSecurityConfig(env);
  const allowedOrigins = config.ALLOWED_ORIGINS;

  let allowedOrigin = 'null';  // 默认拒绝所有跨域请求

  // 只有明确配置了允许的域名才允许跨域
  if (allowedOrigins.length > 0 && origin) {
    // 精确匹配
    if (allowedOrigins.includes(origin)) {
      allowedOrigin = origin;
    } else {
      // 子域名匹配 (*.example.com)
      for (const allowed of allowedOrigins) {
        if (allowed.startsWith('*.')) {
          const domain = allowed.substring(2);
          if (origin === domain || origin.endsWith(`.${domain}`)) {
            allowedOrigin = origin;
            break;
          }
        }
      }
    }
  }

  return {
    'Access-Control-Allow-Origin': allowedOrigin,
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-API-Key',
    'Access-Control-Allow-Credentials': allowedOrigin !== 'null' ? 'true' : 'false',
    'Access-Control-Max-Age': '86400',
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    'X-XSS-Protection': '1; mode=block',
    'Referrer-Policy': 'strict-origin-when-cross-origin',
    'Content-Security-Policy': "default-src 'self'; script-src 'self' https://cdn.jsdelivr.net; style-src 'self' https://cdn.jsdelivr.net; img-src 'self' data: https:; font-src 'self' https://cdn.jsdelivr.net; connect-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none';"
  };
}

// ==================== API路由模块 ====================

// 认证路由处理器
async function handleAuthRoutes(path, method, request, env, corsHeaders, clientIP) {
  // 登录处理
  if (path === '/api/auth/login' && method === 'POST') {
    try {
      if (!checkLoginAttempts(clientIP, env)) {
        return createErrorResponse(
          'Too many login attempts',
          '登录尝试次数过多，请15分钟后再试',
          429,
          corsHeaders
        );
      }

      const { username, password } = await parseJsonSafely(request);
      if (!username || !password) {
        recordLoginAttempt(clientIP);
        return createErrorResponse(
          'Missing credentials',
          '用户名和密码不能为空',
          400,
          corsHeaders
        );
      }

      const user = await env.DB.prepare(
        'SELECT username, password_hash, locked_until, failed_attempts FROM admin_credentials WHERE username = ?'
      ).bind(username).first();

      if (!user) {
        recordLoginAttempt(clientIP);
        return createErrorResponse(
          'Invalid credentials',
          '用户名或密码错误',
          401,
          corsHeaders
        );
      }

      if (user.locked_until && Date.now() < user.locked_until) {
        return createErrorResponse(
          'Account locked',
          '账户已被锁定，请稍后再试',
          423,
          corsHeaders
        );
      }

      const isValidPassword = await verifyPassword(password, user.password_hash);
      if (!isValidPassword) {
        recordLoginAttempt(clientIP);

        const newFailedAttempts = (user.failed_attempts || 0) + 1;
        const config = getSecurityConfig(env);
        let lockedUntil = null;

        if (newFailedAttempts >= config.MAX_LOGIN_ATTEMPTS) {
          lockedUntil = Date.now() + config.LOGIN_ATTEMPT_WINDOW;
        }

        await env.DB.prepare(
          'UPDATE admin_credentials SET failed_attempts = ?, locked_until = ? WHERE username = ?'
        ).bind(newFailedAttempts, lockedUntil, username).run();

        return createErrorResponse(
          'Invalid credentials',
          '用户名或密码错误',
          401,
          corsHeaders
        );
      }

      // 登录成功，重置失败次数
      await env.DB.prepare(
        'UPDATE admin_credentials SET failed_attempts = 0, locked_until = NULL, last_login = ? WHERE username = ?'
      ).bind(Date.now(), username).run();

      const isUsingDefault = await isUsingDefaultPassword(username, password, env);
      const token = await createJWT({ username, usingDefaultPassword: isUsingDefault }, env);

      return createSuccessResponse({
        token,
        user: { username, usingDefaultPassword: isUsingDefault }
      }, corsHeaders);

    } catch (error) {
      return handleDbError(error, corsHeaders, '登录');
    }
  }

  // 认证状态检查
  if (path === '/api/auth/status' && method === 'GET') {
    try {
      const user = await authenticateRequest(request, env);
      if (!user) {
        return createApiResponse({ authenticated: false }, 200, corsHeaders);
      }

      const dbUser = await env.DB.prepare(
        'SELECT username FROM admin_credentials WHERE username = ?'
      ).bind(user.username).first();

      if (!dbUser) {
        return createApiResponse({ authenticated: false }, 200, corsHeaders);
      }

      return createApiResponse({
        authenticated: true,
        user: {
          username: user.username,
          usingDefaultPassword: user.usingDefaultPassword || false
        }
      }, 200, corsHeaders);

    } catch (error) {
      return createApiResponse({ authenticated: false }, 200, corsHeaders);
    }
  }

  // 修改密码
  if (path === '/api/auth/change-password' && method === 'POST') {
    try {
      const user = await authenticateRequest(request, env);
      if (!user) {
        return createErrorResponse('Unauthorized', '需要登录', 401, corsHeaders);
      }

      const { current_password, new_password } = await parseJsonSafely(request);
      if (!current_password || !new_password) {
        return createErrorResponse(
          'Missing fields',
          '当前密码和新密码不能为空',
          400,
          corsHeaders
        );
      }

      const config = getSecurityConfig(env);
      if (new_password.length < config.MIN_PASSWORD_LENGTH) {
        return createErrorResponse(
          'Password too short',
          `密码长度至少为${config.MIN_PASSWORD_LENGTH}位`,
          400,
          corsHeaders
        );
      }

      const dbUser = await env.DB.prepare(
        'SELECT password_hash FROM admin_credentials WHERE username = ?'
      ).bind(user.username).first();

      if (!dbUser || !await verifyPassword(current_password, dbUser.password_hash)) {
        return createErrorResponse(
          'Invalid current password',
          '当前密码错误',
          400,
          corsHeaders
        );
      }

      const newPasswordHash = await hashPassword(new_password);
      await env.DB.prepare(
        'UPDATE admin_credentials SET password_hash = ?, password_changed_at = ?, must_change_password = 0 WHERE username = ?'
      ).bind(newPasswordHash, Date.now(), user.username).run();

      // 撤销当前令牌，强制重新登录
      const authHeader = request.headers.get('Authorization');
      if (authHeader && authHeader.startsWith('Bearer ')) {
        const currentToken = authHeader.substring(7);
        revokeToken(currentToken);
      }

      return createSuccessResponse({
        message: '密码修改成功，请重新登录',
        requireReauth: true
      }, corsHeaders);

    } catch (error) {
      return handleDbError(error, corsHeaders, '修改密码');
    }
  }

  return null; // 不匹配此模块的路由
}

// 服务器管理路由处理器
async function handleServerRoutes(path, method, request, env, corsHeaders) {
  // 获取服务器列表（公开，支持管理员和游客模式）
  if (path === '/api/servers' && method === 'GET') {
    try {
      const user = await authenticateRequestOptional(request, env);
      const isAdmin = user !== null;

      // 使用缓存机制获取服务器列表
      const servers = await configCache.getServerList(env.DB, isAdmin);
      return createApiResponse({ servers }, 200, corsHeaders);

    } catch (error) {
      return handleDbError(error, corsHeaders, '获取服务器列表');
    }
  }

  // 管理员获取服务器列表（包含详细信息）
  if (path === '/api/admin/servers' && method === 'GET') {
    const user = await authenticateAdmin(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const { results } = await env.DB.prepare(`
        SELECT s.id, s.name, s.description, s.created_at, s.sort_order,
               s.last_notified_down_at, s.api_key, s.is_public, m.timestamp as last_report
        FROM servers s
        LEFT JOIN metrics m ON s.id = m.server_id
        ORDER BY s.sort_order ASC NULLS LAST, s.name ASC
      `).all();

      // 检查是否需要完整密钥（用于查看密钥和复制脚本功能）
      const url = new URL(request.url);
      const showFullKey = url.searchParams.get('full_key') === 'true';

      // 根据参数决定是否脱敏API密钥
      const servers = (results || []).map(server => ({
        ...server,
        api_key: showFullKey ? server.api_key : maskSensitive(server.api_key)
      }));

      return createApiResponse({ servers }, 200, corsHeaders);

    } catch (error) {
            return handleDbError(error, corsHeaders, '获取管理员服务器列表');
    }
  }

  // 添加服务器（管理员）
  if (path === '/api/admin/servers' && method === 'POST') {
    const user = await authenticateAdmin(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const { name, description } = await parseJsonSafely(request);
      if (!validateInput(name, 'serverName')) {
        return createErrorResponse(
          'Invalid server name',
          '服务器名称格式无效',
          400,
          corsHeaders
        );
      }

      const serverId = Math.random().toString(36).substring(2, 8);
      // 生成32字节强随机API密钥
      const apiKey = Array.from(crypto.getRandomValues(new Uint8Array(32)), b => b.toString(16).padStart(2, '0')).join('');
      const now = Math.floor(Date.now() / 1000);

      await env.DB.prepare(`
        INSERT INTO servers (id, name, description, api_key, created_at, sort_order, is_public)
        VALUES (?, ?, ?, ?, ?, 0, 1)
      `).bind(serverId, name, description || '', apiKey, now).run();

      // 清除服务器列表缓存
      configCache.clearKey('servers_admin');
      configCache.clearKey('servers_public');

      return createSuccessResponse({
        server: {
          id: serverId,
          name,
          description: description || '',
          api_key: maskSensitive(apiKey),
          created_at: now
        }
      }, corsHeaders);

    } catch (error) {
            return handleDbError(error, corsHeaders, '添加服务器');
    }
  }

  // 更新服务器（管理员） - 修复权限检查
  if (path.match(/\/api\/admin\/servers\/[^\/]+$/) && method === 'PUT') {
    const user = await authenticateAdmin(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const serverId = extractAndValidateServerId(path);
      if (!serverId) {
        return createErrorResponse(
          'Invalid server ID',
          '无效的服务器ID格式',
          400,
          corsHeaders
        );
      }

      const { name, description } = await request.json();
      if (!validateInput(name, 'serverName')) {
        return createErrorResponse(
          'Invalid server name',
          '服务器名称格式无效',
          400,
          corsHeaders
        );
      }

      const info = await env.DB.prepare(`
        UPDATE servers SET name = ?, description = ? WHERE id = ?
      `).bind(name, description || '', serverId).run();

      if (info.changes === 0) {
        return createErrorResponse('Server not found', '服务器不存在', 404, corsHeaders);
      }

      // 清除服务器列表缓存
      configCache.clearKey('servers_admin');
      configCache.clearKey('servers_public');

      return createSuccessResponse({
        id: serverId,
        name,
        description: description || '',
        message: '服务器更新成功'
      }, corsHeaders);

    } catch (error) {
            return handleDbError(error, corsHeaders, '更新服务器');
    }
  }

  // 删除服务器（管理员）
  if (path.match(/\/api\/admin\/servers\/[^\/]+$/) && method === 'DELETE') {
    const user = await authenticateAdmin(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const serverId = extractAndValidateServerId(path);
      if (!serverId) {
        return createErrorResponse(
          'Invalid server ID',
          '无效的服务器ID格式',
          400,
          corsHeaders
        );
      }

      // 危险操作需要确认
      const url = new URL(request.url);
      const confirmed = url.searchParams.get('confirm') === 'true';
      if (!confirmed) {
        return createErrorResponse(
          'Confirmation required',
          '删除操作需要确认，请添加 ?confirm=true 参数',
          400,
          corsHeaders
        );
      }

      const info = await env.DB.prepare('DELETE FROM servers WHERE id = ?').bind(serverId).run();
      if (info.changes === 0) {
        return createErrorResponse('Server not found', '服务器不存在', 404, corsHeaders);
      }

      // 同时删除相关的监控数据
      await env.DB.prepare('DELETE FROM metrics WHERE server_id = ?').bind(serverId).run();

      // 清除服务器列表缓存
      configCache.clearKey('servers_admin');
      configCache.clearKey('servers_public');

      return createSuccessResponse({ message: '服务器已删除' }, corsHeaders);

    } catch (error) {
            return handleDbError(error, corsHeaders, '删除服务器');
    }
  }

  return null; // 不匹配此模块的路由
}

// VPS监控路由处理器
async function handleVpsRoutes(path, method, request, env, corsHeaders, ctx) {
  // VPS配置获取（使用API密钥认证）
  if (path.startsWith('/api/config/') && method === 'GET') {
    try {
      const authResult = await validateServerAuth(path, request, env);
      if (!authResult.success) {
        return createErrorResponse(authResult.error, authResult.message,
          authResult.error === 'Invalid server ID' ? 400 : 401, corsHeaders);
      }

      const { serverId, serverData } = authResult;
      const reportInterval = await getVpsReportInterval(env);

      const configData = {
        success: true,
        config: {
          report_interval: reportInterval,
          enabled_metrics: ['cpu', 'memory', 'disk', 'network', 'uptime'],
          server_info: {
            id: serverData.id,
            name: serverData.name,
            description: serverData.description || ''
          }
        },
        timestamp: Math.floor(Date.now() / 1000)
      };

      return createApiResponse(configData, 200, corsHeaders);

    } catch (error) {
            return handleDbError(error, corsHeaders, '配置获取');
    }
  }

  // VPS数据上报
  if (path.startsWith('/api/report/') && method === 'POST') {
    try {
      const authResult = await validateServerAuth(path, request, env);
      if (!authResult.success) {
        return createErrorResponse(authResult.error, authResult.message,
          authResult.error === 'Invalid server ID' ? 400 : 401, corsHeaders);
      }

      const { serverId } = authResult;

      // 解析和验证上报数据
      let reportData;
      try {
        const rawBody = await request.text();
        reportData = JSON.parse(rawBody);
      } catch (parseError) {
        return createErrorResponse(
          'Invalid JSON format',
          `JSON解析失败: ${parseError.message}`,
          400,
          corsHeaders,
          '请检查上报的JSON格式是否正确'
        );
      }

      const validationResult = validateAndFixVpsData(reportData);
      if (!validationResult.success) {
        return createErrorResponse(
          validationResult.error,
          validationResult.message,
          400,
          corsHeaders,
          validationResult.details
        );
      }

      reportData = validationResult.data;

      // 获取当前的批量写入间隔（与VPS上报间隔一致）
      const currentInterval = await getVpsReportInterval(env);

      // 使用批量处理器处理VPS数据
      const shouldFlush = vpsBatchProcessor.addReport(serverId, reportData, currentInterval);

      // 如果需要刷新或使用ctx.waitUntil进行异步刷新
      if (shouldFlush) {
        ctx.waitUntil(flushVpsBatchData(env));
      } else {
        // 检查是否有定时需要刷新的数据
        if (vpsBatchProcessor.shouldFlush(currentInterval)) {
          ctx.waitUntil(flushVpsBatchData(env));
        }
      }

      return createSuccessResponse({ interval: currentInterval }, corsHeaders);

    } catch (error) {
            return handleDbError(error, corsHeaders, '数据上报');
    }
  }

  // 批量VPS状态查询（公开，支持管理员和游客模式）
  if (path === '/api/status/batch' && method === 'GET') {
    try {
      const user = await authenticateRequestOptional(request, env);
      const isAdmin = user !== null;

      // 使用JOIN查询一次性获取所有VPS状态
      const { results } = await env.DB.prepare(`
        SELECT s.id, s.name, s.description,
               m.timestamp, m.cpu, m.memory, m.disk, m.network, m.uptime
        FROM servers s
        LEFT JOIN metrics m ON s.id = m.server_id
        WHERE s.is_public = 1 OR ? = 1
        ORDER BY s.sort_order ASC NULLS LAST, s.name ASC
      `).bind(isAdmin ? 1 : 0).all();

      // 处理数据格式，保持与单个查询API的兼容性
      const servers = (results || []).map(row => {
        const server = { id: row.id, name: row.name, description: row.description };
        let metrics = null;

        if (row.timestamp) {
          metrics = {
            timestamp: row.timestamp,
            uptime: row.uptime
          };

          // 解析JSON字段
          try {
            if (row.cpu) metrics.cpu = JSON.parse(row.cpu);
            if (row.memory) metrics.memory = JSON.parse(row.memory);
            if (row.disk) metrics.disk = JSON.parse(row.disk);
            if (row.network) metrics.network = JSON.parse(row.network);
          } catch (parseError) {
            // 静默处理JSON解析错误
          }
        }

        return { server, metrics, error: false };
      });

      return createApiResponse({ servers }, 200, corsHeaders);

    } catch (error) {
            return handleDbError(error, corsHeaders, '批量VPS状态查询');
    }
  }

  // VPS状态查询（公开，无需认证）
  if (path.startsWith('/api/status/') && method === 'GET') {
    try {
      const serverId = path.split('/')[3]; // 从 /api/status/{serverId} 提取ID
      if (!serverId) {
        return createErrorResponse('Invalid server ID', '无效的服务器ID', 400, corsHeaders);
      }

      // 查询服务器信息（移除权限限制，让前台能正常显示）
      const serverData = await env.DB.prepare(
        'SELECT id, name, description FROM servers WHERE id = ?'
      ).bind(serverId).first();

      if (!serverData) {
        return createErrorResponse('Server not found', '服务器不存在', 404, corsHeaders);
      }

      // 查询最新的VPS监控数据
      const metricsData = await env.DB.prepare(`
        SELECT * FROM metrics
        WHERE server_id = ?
        ORDER BY timestamp DESC
        LIMIT 1
      `).bind(serverId).first();

      // 解析JSON字符串为对象
      if (metricsData) {
        try {
          if (metricsData.cpu) metricsData.cpu = JSON.parse(metricsData.cpu);
          if (metricsData.memory) metricsData.memory = JSON.parse(metricsData.memory);
          if (metricsData.disk) metricsData.disk = JSON.parse(metricsData.disk);
          if (metricsData.network) metricsData.network = JSON.parse(metricsData.network);
        } catch (parseError) {
          // 静默处理JSON解析错误
        }
      }

      // 返回完整的监控数据（保持前端兼容性）
      const publicInfo = {
        server: serverData,
        metrics: metricsData || null,
        error: false
      };

      return createApiResponse(publicInfo, 200, corsHeaders);

    } catch (error) {
            return handleDbError(error, corsHeaders, 'VPS状态查询');
    }
  }

  // VPS状态变化通知API
  if (path === '/api/notify/offline' && method === 'POST') {
    try {
      const { serverId, serverName } = await request.json();

      // 检查是否已发送过离线通知
      const server = await env.DB.prepare('SELECT last_notified_down_at FROM servers WHERE id = ?').bind(serverId).first();
      if (server?.last_notified_down_at) {
        return createApiResponse({ success: true, message: 'Already notified' }, 200, corsHeaders);
      }

      const message = `🔴 VPS故障: 服务器 *${serverName}* 已离线超过5分钟`;

      // 记录离线时间并发送通知
      await env.DB.prepare('UPDATE servers SET last_notified_down_at = ? WHERE id = ?')
        .bind(Math.floor(Date.now() / 1000), serverId).run();
      ctx.waitUntil(sendNotifications(env.DB, message, 'high'));

      return createApiResponse({ success: true }, 200, corsHeaders);
    } catch (error) {
            return createErrorResponse('Notification failed', '通知发送失败', 500, corsHeaders);
    }
  }

  if (path === '/api/notify/recovery' && method === 'POST') {
    try {
      const { serverId, serverName } = await request.json();
      const message = `✅ VPS恢复: 服务器 *${serverName}* 已恢复在线`;

      // 清除离线记录
      await env.DB.prepare('UPDATE servers SET last_notified_down_at = NULL WHERE id = ?')
        .bind(serverId).run();

      // 发送通知
      ctx.waitUntil(sendNotifications(env.DB, message, 'high'));

      return createApiResponse({ success: true }, 200, corsHeaders);
    } catch (error) {
            return createErrorResponse('Notification failed', '通知发送失败', 500, corsHeaders);
    }
  }

  return null; // 不匹配此模块的路由
}

// ==================== API请求处理 ====================

async function handleApiRequest(request, env, ctx) {
  const url = new URL(request.url);
  const path = url.pathname;
  const method = request.method;
  const clientIP = getClientIP(request);
  const origin = request.headers.get('Origin');
  const corsHeaders = getSecureCorsHeaders(origin, env);

  // OPTIONS请求处理
  if (method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  // 速率限制检查（登录接口除外）
  if (path !== '/api/auth/login' && !checkRateLimit(clientIP, path, env)) {
    return createErrorResponse(
      'Rate limit exceeded',
      '请求过于频繁，请稍后再试',
      429,
      corsHeaders
    );
  }

  // ==================== 路由分发 ====================

  // 认证相关路由
  if (path.startsWith('/api/auth/')) {
    const authResult = await handleAuthRoutes(path, method, request, env, corsHeaders, clientIP);
    if (authResult) return authResult;
  }

  // 服务器管理路由
  if (path.startsWith('/api/servers') || path.startsWith('/api/admin/servers')) {
    const serverResult = await handleServerRoutes(path, method, request, env, corsHeaders);
    if (serverResult) return serverResult;
  }



  // VPS监控路由
  if (path.startsWith('/api/config/') || path.startsWith('/api/report/') ||
      path.startsWith('/api/status/') || path.startsWith('/api/notify/')) {
    const vpsResult = await handleVpsRoutes(path, method, request, env, corsHeaders, ctx);
    if (vpsResult) return vpsResult;
  }

  // 数据库初始化API（无需认证）
  if (path === '/api/init-db' && ['POST', 'GET'].includes(method)) {
    try {
      await ensureTablesExist(env.DB, env);
      return createSuccessResponse({
        message: '数据库初始化完成'
      }, corsHeaders);
    } catch (error) {
      return createErrorResponse(
        'Database initialization failed',
        `数据库初始化失败: ${error.message}`,
        500,
        corsHeaders
      );
    }
  }


























  // ==================== 高级排序功能 ====================

  // 批量服务器排序（管理员） - 修复权限检查
  if (path === '/api/admin/servers/batch-reorder' && method === 'POST') {
    const user = await authenticateAdmin(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const { serverIds } = await request.json(); // 按新顺序排列的服务器ID数组

      if (!Array.isArray(serverIds) || serverIds.length === 0) {
        return new Response(JSON.stringify({
          error: 'Invalid server IDs',
          message: '服务器ID数组无效'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      // 批量更新排序
      const updateStmts = serverIds.map((serverId, index) =>
        env.DB.prepare('UPDATE servers SET sort_order = ? WHERE id = ?').bind(index, serverId)
      );

      await env.DB.batch(updateStmts);

      return new Response(JSON.stringify({
        success: true,
        message: '批量排序完成'
      }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
            return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // 自动服务器排序（管理员） - 修复权限检查
  if (path === '/api/admin/servers/auto-sort' && method === 'POST') {
    const user = await authenticateAdmin(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const { sortBy, order } = await request.json(); // sortBy: 'custom'|'name'|'status', order: 'asc'|'desc'

      const validSortFields = ['custom', 'name', 'status'];
      const validOrders = ['asc', 'desc'];

      if (!validSortFields.includes(sortBy) || !validOrders.includes(order)) {
        return new Response(JSON.stringify({
          error: 'Invalid sort parameters',
          message: '无效的排序参数'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      // 如果是自定义排序，直接返回成功，不做任何操作
      if (sortBy === 'custom') {
        return new Response(JSON.stringify({
          success: true,
          message: '已设置为自定义排序'
        }), {
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      // 获取所有服务器并排序 - 安全验证
      const safeOrder = validateSqlIdentifier(order.toUpperCase(), 'order');
      let orderClause = '';
      if (sortBy === 'name') {
        orderClause = `ORDER BY name ${safeOrder}`;
      } else if (sortBy === 'status') {
        orderClause = `ORDER BY (CASE WHEN m.timestamp IS NULL OR (strftime('%s', 'now') - m.timestamp) > 300 THEN 1 ELSE 0 END) ${safeOrder}, name ASC`;
      }

      const { results: servers } = await env.DB.prepare(`
        SELECT s.id FROM servers s
        LEFT JOIN metrics m ON s.id = m.server_id
        ${orderClause}
      `).all();

      // 批量更新排序
      const updateStmts = servers.map((server, index) =>
        env.DB.prepare('UPDATE servers SET sort_order = ? WHERE id = ?').bind(index, server.id)
      );

      await env.DB.batch(updateStmts);

      return new Response(JSON.stringify({
        success: true,
        message: `已按${sortBy}${order === 'asc' ? '升序' : '降序'}排序`
      }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
            return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // 服务器排序（管理员）- 保留原有的单个移动功能
  if (path.match(/\/api\/admin\/servers\/[^\/]+\/reorder$/) && method === 'POST') {
    try {
      const serverId = extractPathSegment(path, 4);
      if (!serverId) {
        return new Response(JSON.stringify({
          error: 'Invalid server ID',
          message: '无效的服务器ID格式'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      const { direction } = await request.json();
      if (!['up', 'down'].includes(direction)) {
        return new Response(JSON.stringify({
          error: 'Invalid direction'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      // 获取所有服务器排序信息
      const results = await env.DB.batch([
        env.DB.prepare('SELECT id, sort_order FROM servers ORDER BY sort_order ASC NULLS LAST, name ASC')
      ]);

      const allServers = results[0].results;
      const currentIndex = allServers.findIndex(s => s.id === serverId);

      if (currentIndex === -1) {
        return new Response(JSON.stringify({
          error: 'Server not found'
        }), {
          status: 404,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      // 计算目标位置
      let targetIndex = -1;
      if (direction === 'up' && currentIndex > 0) {
        targetIndex = currentIndex - 1;
      } else if (direction === 'down' && currentIndex < allServers.length - 1) {
        targetIndex = currentIndex + 1;
      }

      if (targetIndex !== -1) {
        const currentServer = allServers[currentIndex];
        const targetServer = allServers[targetIndex];

        // 处理排序值交换
        if (currentServer.sort_order === null || targetServer.sort_order === null) {
                    const updateStmts = allServers.map((server, index) =>
            env.DB.prepare('UPDATE servers SET sort_order = ? WHERE id = ?').bind(index, server.id)
          );
          await env.DB.batch(updateStmts);

          // 重新获取并交换
          const updatedResults = await env.DB.batch([
            env.DB.prepare('SELECT id, sort_order FROM servers ORDER BY sort_order ASC')
          ]);
          const updatedServers = updatedResults[0].results;
          const newCurrentIndex = updatedServers.findIndex(s => s.id === serverId);
          let newTargetIndex = -1;

          if (direction === 'up' && newCurrentIndex > 0) {
            newTargetIndex = newCurrentIndex - 1;
          } else if (direction === 'down' && newCurrentIndex < updatedServers.length - 1) {
            newTargetIndex = newCurrentIndex + 1;
          }

          if (newTargetIndex !== -1) {
            const newCurrentOrder = updatedServers[newCurrentIndex].sort_order;
            const newTargetOrder = updatedServers[newTargetIndex].sort_order;
            await env.DB.batch([
              env.DB.prepare('UPDATE servers SET sort_order = ? WHERE id = ?').bind(newTargetOrder, serverId),
              env.DB.prepare('UPDATE servers SET sort_order = ? WHERE id = ?').bind(newCurrentOrder, updatedServers[newTargetIndex].id)
            ]);
          }
        } else {
          // 直接交换排序值
          await env.DB.batch([
            env.DB.prepare('UPDATE servers SET sort_order = ? WHERE id = ?').bind(targetServer.sort_order, serverId),
            env.DB.prepare('UPDATE servers SET sort_order = ? WHERE id = ?').bind(currentServer.sort_order, targetServer.id)
          ]);
        }
      }

      return createSuccessResponse({}, corsHeaders);
    } catch (error) {
            return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // 更新服务器显示状态（管理员）
  if (path.match(/^\/api\/admin\/servers\/([^\/]+)\/visibility$/) && method === 'POST') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const serverId = path.split('/')[4];
      const { is_public } = await request.json();

      // 验证输入
      if (typeof is_public !== 'boolean') {
        return new Response(JSON.stringify({
          error: 'Invalid input',
          message: '显示状态必须为布尔值'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      // 更新服务器显示状态
      await env.DB.prepare(`
        UPDATE servers SET is_public = ? WHERE id = ?
      `).bind(is_public ? 1 : 0, serverId).run();

      return createSuccessResponse({}, corsHeaders);
    } catch (error) {
            return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }



  // ==================== 网站监控API ====================

  // 获取监控站点列表（管理员）
  if (path === '/api/admin/sites' && method === 'GET') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const { results } = await env.DB.prepare(`
        SELECT id, name, url, added_at, last_checked, last_status, last_status_code,
               last_response_time_ms, sort_order, last_notified_down_at, is_public
        FROM monitored_sites
        ORDER BY sort_order ASC NULLS LAST, name ASC, url ASC
      `).all();

      return new Response(JSON.stringify({ sites: results || [] }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
            if (error.message.includes('no such table')) {
                try {
          await env.DB.exec(D1_SCHEMAS.monitored_sites);
          return new Response(JSON.stringify({ sites: [] }), {
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
          });
        } catch (createError) {
                  }
      }
      return new Response(JSON.stringify({
        error: 'Internal server error',
        message: '服务器内部错误'
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }
  }

  // 添加监控站点（管理员）
  if (path === '/api/admin/sites' && method === 'POST') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const { url, name } = await parseJsonSafely(request);

      if (!url || !isValidHttpUrl(url)) {
        return new Response(JSON.stringify({
          error: 'Valid URL is required',
          message: '请输入有效的URL'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      const siteId = Math.random().toString(36).substring(2, 12);
      const addedAt = Math.floor(Date.now() / 1000);

      // 获取下一个排序序号
      const maxOrderResult = await env.DB.prepare(
        'SELECT MAX(sort_order) as max_order FROM monitored_sites'
      ).first();
      const nextSortOrder = (maxOrderResult?.max_order && typeof maxOrderResult.max_order === 'number')
        ? maxOrderResult.max_order + 1
        : 0;

      await env.DB.prepare(`
        INSERT INTO monitored_sites (id, url, name, added_at, last_status, sort_order)
        VALUES (?, ?, ?, ?, ?, ?)
      `).bind(siteId, url, name || '', addedAt, 'PENDING', nextSortOrder).run();

      const siteData = {
        id: siteId,
        url,
        name: name || '',
        added_at: addedAt,
        last_status: 'PENDING',
        sort_order: nextSortOrder
      };

      // 立即执行健康检查
      const newSiteForCheck = { id: siteId, url, name: name || '' };
      if (ctx?.waitUntil) {
        ctx.waitUntil(checkWebsiteStatusOptimized(newSiteForCheck, env.DB, ctx));

      } else {
        checkWebsiteStatusOptimized(newSiteForCheck, env.DB, ctx).catch(e => {
          // 静默处理站点检查错误
        });
      }

      return new Response(JSON.stringify({ site: siteData }), {
        status: 201,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
            if (error.message.includes('UNIQUE constraint failed')) {
        return new Response(JSON.stringify({
          error: 'URL already exists or ID conflict',
          message: '该URL已被监控或ID冲突'
        }), {
          status: 409,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      if (error.message.includes('no such table')) {
                try {
          await env.DB.exec(D1_SCHEMAS.monitored_sites);
          return new Response(JSON.stringify({
            error: 'Database table created, please retry',
            message: '数据库表已创建，请重试添加操作'
          }), {
            status: 503,
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
          });
        } catch (createError) {
                  }
      }

      return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }



  // 更新监控站点（管理员）
  if (path.match(/\/api\/admin\/sites\/[^\/]+$/) && method === 'PUT') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const siteId = path.split('/').pop();
      if (!siteId) {
        return createErrorResponse('Invalid site ID', '无效的网站ID', 400, corsHeaders);
      }

      const { url, name } = await request.json();
      if (!url || !url.trim()) {
        return createErrorResponse('Invalid URL', 'URL不能为空', 400, corsHeaders);
      }

      if (!url.startsWith('http://') && !url.startsWith('https://')) {
        return createErrorResponse('Invalid URL format', 'URL必须以http://或https://开头', 400, corsHeaders);
      }

      const info = await env.DB.prepare(`
        UPDATE monitored_sites SET url = ?, name = ? WHERE id = ?
      `).bind(url.trim(), name?.trim() || '', siteId).run();

      if (info.changes === 0) {
        return createErrorResponse('Site not found', '网站不存在', 404, corsHeaders);
      }

      return createSuccessResponse({
        id: siteId,
        url: url.trim(),
        name: name?.trim() || '',
        message: '网站更新成功'
      }, corsHeaders);

    } catch (error) {
            return handleDbError(error, corsHeaders, '更新监控站点');
    }
  }

  // 删除监控站点（管理员）
  if (path.match(/\/api\/admin\/sites\/[^\/]+$/) && method === 'DELETE') {
    const user = await authenticateAdmin(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const siteId = extractAndValidateServerId(path);
      if (!siteId) {
        return createErrorResponse('Invalid site ID', '无效的站点ID格式', 400, corsHeaders);
      }

      // 危险操作需要确认
      const url = new URL(request.url);
      const confirmed = url.searchParams.get('confirm') === 'true';
      if (!confirmed) {
        return createErrorResponse(
          'Confirmation required',
          '删除操作需要确认，请添加 ?confirm=true 参数',
          400,
          corsHeaders
        );
      }

      const info = await env.DB.prepare('DELETE FROM monitored_sites WHERE id = ?').bind(siteId).run();

      if (info.changes === 0) {
        return new Response(JSON.stringify({
          error: 'Site not found'
        }), {
          status: 404,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      return createSuccessResponse({}, corsHeaders);
    } catch (error) {
            return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // 批量网站排序（管理员）
  if (path === '/api/admin/sites/batch-reorder' && method === 'POST') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const { siteIds } = await request.json(); // 按新顺序排列的站点ID数组

      if (!Array.isArray(siteIds) || siteIds.length === 0) {
        return new Response(JSON.stringify({
          error: 'Invalid site IDs',
          message: '站点ID数组无效'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      // 批量更新排序
      const updateStmts = siteIds.map((siteId, index) =>
        env.DB.prepare('UPDATE monitored_sites SET sort_order = ? WHERE id = ?').bind(index, siteId)
      );

      await env.DB.batch(updateStmts);

      return new Response(JSON.stringify({
        success: true,
        message: '批量排序完成'
      }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
            return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // 自动网站排序（管理员）
  if (path === '/api/admin/sites/auto-sort' && method === 'POST') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const { sortBy, order } = await request.json(); // sortBy: 'custom'|'name'|'url'|'status', order: 'asc'|'desc'

      const validSortFields = ['custom', 'name', 'url', 'status'];
      const validOrders = ['asc', 'desc'];

      if (!validSortFields.includes(sortBy) || !validOrders.includes(order)) {
        return new Response(JSON.stringify({
          error: 'Invalid sort parameters',
          message: '无效的排序参数'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      // 如果是自定义排序，直接返回成功，不做任何操作
      if (sortBy === 'custom') {
        return new Response(JSON.stringify({
          success: true,
          message: '已设置为自定义排序'
        }), {
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      // 获取所有站点并排序 - 安全验证
      const safeSortBy = validateSqlIdentifier(sortBy, 'column');
      const safeOrder = validateSqlIdentifier(order.toUpperCase(), 'order');

      const { results: sites } = await env.DB.prepare(`
        SELECT id FROM monitored_sites
        ORDER BY ${safeSortBy} ${safeOrder}
      `).all();

      // 批量更新排序
      const updateStmts = sites.map((site, index) =>
        env.DB.prepare('UPDATE monitored_sites SET sort_order = ? WHERE id = ?').bind(index, site.id)
      );

      await env.DB.batch(updateStmts);

      return new Response(JSON.stringify({
        success: true,
        message: `已按${sortBy}${order === 'asc' ? '升序' : '降序'}排序`
      }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
            return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // 网站排序（管理员）- 保留原有的单个移动功能
  if (path.match(/\/api\/admin\/sites\/[^\/]+\/reorder$/) && method === 'POST') {
    try {
      const siteId = extractPathSegment(path, 4);
      if (!siteId) {
        return new Response(JSON.stringify({
          error: 'Invalid site ID',
          message: '无效的站点ID格式'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      const { direction } = await request.json();
      if (!['up', 'down'].includes(direction)) {
        return new Response(JSON.stringify({
          error: 'Invalid direction'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      // 获取所有站点排序信息
      const results = await env.DB.batch([
        env.DB.prepare('SELECT id, sort_order FROM monitored_sites ORDER BY sort_order ASC NULLS LAST, name ASC, url ASC')
      ]);
      const allSites = results[0].results;
      const currentIndex = allSites.findIndex(s => s.id === siteId);

      if (currentIndex === -1) {
        return new Response(JSON.stringify({
          error: 'Site not found'
        }), {
          status: 404,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      // 计算目标位置
      let targetIndex = -1;
      if (direction === 'up' && currentIndex > 0) {
        targetIndex = currentIndex - 1;
      } else if (direction === 'down' && currentIndex < allSites.length - 1) {
        targetIndex = currentIndex + 1;
      }

      if (targetIndex !== -1) {
        const currentSite = allSites[currentIndex];
        const targetSite = allSites[targetIndex];

        // 处理排序值交换
        if (currentSite.sort_order === null || targetSite.sort_order === null) {
                    const updateStmts = allSites.map((site, index) =>
            env.DB.prepare('UPDATE monitored_sites SET sort_order = ? WHERE id = ?').bind(index, site.id)
          );
          await env.DB.batch(updateStmts);

          // 重新获取并交换
          const updatedResults = await env.DB.batch([
            env.DB.prepare('SELECT id, sort_order FROM monitored_sites ORDER BY sort_order ASC')
          ]);
          const updatedSites = updatedResults[0].results;
          const newCurrentIndex = updatedSites.findIndex(s => s.id === siteId);
          let newTargetIndex = -1;

          if (direction === 'up' && newCurrentIndex > 0) {
            newTargetIndex = newCurrentIndex - 1;
          } else if (direction === 'down' && newCurrentIndex < updatedSites.length - 1) {
            newTargetIndex = newCurrentIndex + 1;
          }

          if (newTargetIndex !== -1) {
            const newCurrentOrder = updatedSites[newCurrentIndex].sort_order;
            const newTargetOrder = updatedSites[newTargetIndex].sort_order;
            await env.DB.batch([
              env.DB.prepare('UPDATE monitored_sites SET sort_order = ? WHERE id = ?').bind(newTargetOrder, siteId),
              env.DB.prepare('UPDATE monitored_sites SET sort_order = ? WHERE id = ?').bind(newCurrentOrder, updatedSites[newTargetIndex].id)
            ]);
          }
        } else {
          // 直接交换排序值
          await env.DB.batch([
            env.DB.prepare('UPDATE monitored_sites SET sort_order = ? WHERE id = ?').bind(targetSite.sort_order, siteId),
            env.DB.prepare('UPDATE monitored_sites SET sort_order = ? WHERE id = ?').bind(currentSite.sort_order, targetSite.id)
          ]);
        }
      }

      return createSuccessResponse({}, corsHeaders);
    } catch (error) {
            return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // 更新网站显示状态（管理员）
  if (path.match(/^\/api\/admin\/sites\/([^\/]+)\/visibility$/) && method === 'POST') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const siteId = path.split('/')[4];
      const { is_public } = await request.json();

      // 验证输入
      if (typeof is_public !== 'boolean') {
        return new Response(JSON.stringify({
          error: 'Invalid input',
          message: '显示状态必须为布尔值'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      // 更新网站显示状态
      await env.DB.prepare(`
        UPDATE monitored_sites SET is_public = ? WHERE id = ?
      `).bind(is_public ? 1 : 0, siteId).run();

      return createSuccessResponse({}, corsHeaders);
    } catch (error) {
            return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }


  // ==================== LLM端点监控API ====================

  // 获取所有LLM端点（管理员）
  if (path === '/api/admin/llm-endpoints' && method === 'GET') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response(JSON.stringify({ error: 'Unauthorized', message: '需要管理员权限' }), {
        status: 401, headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    try {
      const { results } = await env.DB.prepare(`
        SELECT id, name, api_url, api_key, model, check_prompt, expected_contains, timeout_ms,
               added_at, last_checked, last_status, last_status_code, last_response_time_ms,
               last_output_preview, sort_order, last_notified_down_at, is_public, enable_notifications
        FROM monitored_llm_endpoints
        ORDER BY sort_order ASC NULLS LAST, model ASC
      `).all();

      return new Response(JSON.stringify({ endpoints: results || [] }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
      if (error.message.includes('no such table')) {
        try {
          await env.DB.exec(D1_SCHEMAS.monitored_llm_endpoints);
          await env.DB.exec(D1_SCHEMAS.llm_status_history);
          return new Response(JSON.stringify({ endpoints: [] }), {
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
          });
        } catch (createError) {}
      }
      return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // 将模型输入解析为模型名称数组（后端通用工具）
  function parseModelsInput(model, models) {
    let modelList = [];
    if (Array.isArray(models)) {
      modelList = models.map(m => {
        if (typeof m === 'string') return m.trim();
        if (m && typeof m === 'object' && m.model) return m.model.trim();
        return '';
      }).filter(Boolean);
    } else if (typeof models === 'string' && models.trim()) {
      // 尝试 JSON 数组字符串
      if (models.trim().startsWith('[')) {
        try {
          const parsed = JSON.parse(models.trim());
          if (Array.isArray(parsed)) {
            modelList = parsed.map(m => typeof m === 'string' ? m.trim() : (m.model || '').trim()).filter(Boolean);
          }
        } catch (e) { /* fall through */ }
      }
      if (modelList.length === 0) {
        modelList = models.split(/[\n,]+/).map(m => m.trim()).filter(Boolean);
      }
    }
    // 去重
    modelList = [...new Set(modelList)];
    // 回退到单 model 字段
    if (modelList.length === 0 && model && model.trim()) {
      modelList = [model.trim()];
    }
    return modelList;
  }

  // 添加LLM端点（管理员）
  if (path === '/api/admin/llm-endpoints' && method === 'POST') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response(JSON.stringify({ error: 'Unauthorized', message: '需要管理员权限' }), {
        status: 401, headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    try {
      const { name, api_url, api_key, model, models, check_prompt, expected_contains, timeout_ms } = await parseJsonSafely(request);

      if (!api_url || !isValidHttpUrl(api_url)) {
        return new Response(JSON.stringify({ error: 'Valid API URL is required', message: '请输入有效的API URL' }), {
          status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      const modelList = parseModelsInput(model, models);
      if (modelList.length === 0) {
        return new Response(JSON.stringify({ error: 'Model is required', message: '请输入至少一个模型名称' }), {
          status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      const addedAt = Math.floor(Date.now() / 1000);
      const checkPrompt = check_prompt || 'Say hello in one word';
      const expectedContains = expected_contains || '';
      const { llmHighLatencyMs } = await getMonitoringThresholds(env.DB);
      const timeoutMs = normalizeLlmTimeout(timeout_ms, llmHighLatencyMs);
      const apiKey = api_key || '';
      const providerName = (typeof name === 'string' ? name.trim() : '') || await getExistingProviderName(env.DB, api_url);

      // 获取当前最大 sort_order
      const maxOrderResult = await env.DB.prepare(
        'SELECT MAX(sort_order) as max_order FROM monitored_llm_endpoints'
      ).first();
      let nextSortOrder = (maxOrderResult?.max_order && typeof maxOrderResult.max_order === 'number')
        ? maxOrderResult.max_order + 1 : 0;

      const insertStmts = [];
      const newEndpoints = [];

      for (const mdl of modelList) {
        const endpointId = Math.random().toString(36).substring(2, 12);
        insertStmts.push(
          env.DB.prepare(`
            INSERT INTO monitored_llm_endpoints (id, name, api_url, api_key, model, check_prompt, expected_contains, timeout_ms, added_at, last_status, sort_order)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `).bind(endpointId, providerName, api_url, apiKey, mdl, checkPrompt, expectedContains, timeoutMs, addedAt, 'PENDING', nextSortOrder)
        );
        newEndpoints.push({
          id: endpointId, name: providerName, api_url, api_key: apiKey,
          model: mdl, check_prompt: checkPrompt,
          expected_contains: expectedContains,
          timeout_ms: timeoutMs,
          added_at: addedAt, last_status: 'PENDING', sort_order: nextSortOrder
        });
        nextSortOrder++;
      }

      // 批量插入
      await env.DB.batch(insertStmts);

      // 为每个新端点触发健康检查
      for (const ep of newEndpoints) {
        if (ctx?.waitUntil) {
          ctx.waitUntil(checkLlmEndpointStatus(ep, env.DB, ctx));
        } else {
          checkLlmEndpointStatus(ep, env.DB, ctx).catch(() => {});
        }
      }

      // 返回兼容格式
      return new Response(JSON.stringify({
        endpoint: newEndpoints[0],
        endpoints: newEndpoints,
        count: newEndpoints.length
      }), {
        status: 201, headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
      if (error.message.includes('UNIQUE constraint failed')) {
        return new Response(JSON.stringify({ error: 'Duplicate entry', message: '该端点已存在' }), {
          status: 409, headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }
      if (error.message.includes('no such table')) {
        try {
          await env.DB.exec(D1_SCHEMAS.monitored_llm_endpoints);
          await env.DB.exec(D1_SCHEMAS.llm_status_history);
          return new Response(JSON.stringify({ error: 'Database table created, please retry', message: '数据库表已创建，请重试' }), {
            status: 503, headers: { 'Content-Type': 'application/json', ...corsHeaders }
          });
        } catch (createError) {}
      }
      return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // 批量添加LLM端点（管理员）
  if (path === '/api/admin/llm-endpoints/batch' && method === 'POST') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response(JSON.stringify({ error: 'Unauthorized', message: '需要管理员权限' }), {
        status: 401, headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    try {
      const body = await parseJsonSafely(request);
      const endpoints = body.endpoints;

      if (!Array.isArray(endpoints) || endpoints.length === 0) {
        return new Response(JSON.stringify({ error: 'Invalid request', message: '端点数组不能为空' }), {
          status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      // 验证所有端点
      for (let i = 0; i < endpoints.length; i++) {
        const ep = endpoints[i];
        if (!ep.api_url || !isValidHttpUrl(ep.api_url)) {
          return new Response(JSON.stringify({
            error: 'Validation failed',
            message: `第 ${i + 1} 个端点: 请输入有效的API URL`
          }), {
            status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders }
          });
        }
        if (!ep.model || !ep.model.trim()) {
          return new Response(JSON.stringify({
            error: 'Validation failed',
            message: `第 ${i + 1} 个端点: 请输入模型名称`
          }), {
            status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders }
          });
        }
      }

      // 获取当前最大 sort_order
      const maxOrderResult = await env.DB.prepare(
        'SELECT MAX(sort_order) as max_order FROM monitored_llm_endpoints'
      ).first();
      let nextSortOrder = (maxOrderResult?.max_order && typeof maxOrderResult.max_order === 'number')
        ? maxOrderResult.max_order + 1 : 0;

      const addedAt = Math.floor(Date.now() / 1000);
      const insertStmts = [];
      const newEndpoints = [];
      const { llmHighLatencyMs } = await getMonitoringThresholds(env.DB);

      for (const ep of endpoints) {
        const endpointId = Math.random().toString(36).substring(2, 12);
        const providerName = (typeof ep.name === 'string' ? ep.name.trim() : '') || await getExistingProviderName(env.DB, ep.api_url);
        const apiUrl = ep.api_url;
        const apiKey = ep.api_key || '';
        const model = ep.model.trim();
        const checkPrompt = ep.check_prompt || 'Say hello in one word';
        const expectedContains = ep.expected_contains || '';
        const timeoutMs = normalizeLlmTimeout(ep.timeout_ms, llmHighLatencyMs);

        insertStmts.push(
          env.DB.prepare(`
            INSERT INTO monitored_llm_endpoints (id, name, api_url, api_key, model, check_prompt, expected_contains, timeout_ms, added_at, last_status, sort_order)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `).bind(
            endpointId, providerName, apiUrl, apiKey, model,
            checkPrompt, expectedContains,
            timeoutMs, addedAt, 'PENDING', nextSortOrder
          )
        );

        newEndpoints.push({
          id: endpointId, name: providerName, api_url: apiUrl, api_key: apiKey,
          model, check_prompt: checkPrompt,
          expected_contains: expectedContains,
          timeout_ms: timeoutMs,
          added_at: addedAt, last_status: 'PENDING', sort_order: nextSortOrder
        });

        nextSortOrder++;
      }

      // 批量插入
      await env.DB.batch(insertStmts);

      // 为每个新端点触发健康检查
      for (const ep of newEndpoints) {
        if (ctx?.waitUntil) {
          ctx.waitUntil(checkLlmEndpointStatus(ep, env.DB, ctx));
        } else {
          checkLlmEndpointStatus(ep, env.DB, ctx).catch(() => {});
        }
      }

      return new Response(JSON.stringify({
        success: true,
        count: newEndpoints.length,
        message: `成功添加 ${newEndpoints.length} 个LLM端点`
      }), {
        status: 201, headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
      if (error.message.includes('UNIQUE constraint failed')) {
        return new Response(JSON.stringify({ error: 'Duplicate entry', message: '存在重复的端点' }), {
          status: 409, headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }
      if (error.message.includes('no such table')) {
        try {
          await env.DB.exec(D1_SCHEMAS.monitored_llm_endpoints);
          await env.DB.exec(D1_SCHEMAS.llm_status_history);
          return new Response(JSON.stringify({ error: 'Database table created, please retry', message: '数据库表已创建，请重试' }), {
            status: 503, headers: { 'Content-Type': 'application/json', ...corsHeaders }
          });
        } catch (createError) {}
      }
      return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // 批量重排LLM端点（管理员）
  if (path === '/api/admin/llm-endpoints/batch-reorder' && method === 'POST') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const { endpointIds } = await parseJsonSafely(request);
      if (!Array.isArray(endpointIds) || endpointIds.length === 0) {
        return createErrorResponse('Invalid request', '端点ID数组无效', 400, corsHeaders);
      }

      const dedupedEndpointIds = [...new Set(endpointIds)];
      const updateStmts = dedupedEndpointIds.map((id, index) =>
        env.DB.prepare('UPDATE monitored_llm_endpoints SET sort_order = ? WHERE id = ?').bind(index, id)
      );

      await env.DB.batch(updateStmts);

      return createSuccessResponse({ message: '排序已更新' }, corsHeaders);
    } catch (error) {
      return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // 更新LLM端点（管理员）
  if (path.match(/\/api\/admin\/llm-endpoints\/[^\/]+$/) && method === 'PUT') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const endpointId = path.split('/').pop();
      if (!endpointId) {
        return createErrorResponse('Invalid endpoint ID', '无效的端点ID', 400, corsHeaders);
      }

      const { name, api_url, api_key, model, models, check_prompt, expected_contains, timeout_ms } = await request.json();
      if (!api_url || !api_url.trim()) {
        return createErrorResponse('Invalid API URL', 'API URL不能为空', 400, corsHeaders);
      }

      const modelList = parseModelsInput(model, models);
      if (modelList.length === 0) {
        return createErrorResponse('Invalid model', '请输入至少一个模型名称', 400, corsHeaders);
      }

      const apiUrl = api_url.trim();
      const apiKey = api_key || '';
      const checkPrompt = check_prompt || 'Say hello in one word';
      const expectedContains = expected_contains || '';
      const { llmHighLatencyMs } = await getMonitoringThresholds(env.DB);
      const timeoutMs = normalizeLlmTimeout(timeout_ms, llmHighLatencyMs);
      const providerName = (typeof name === 'string' ? name.trim() : '') || await getExistingProviderName(env.DB, apiUrl);

      // 第一个模型：更新当前端点
      const info = await env.DB.prepare(`
        UPDATE monitored_llm_endpoints SET name = ?, api_url = ?, api_key = ?, model = ?, check_prompt = ?, expected_contains = ?, timeout_ms = ? WHERE id = ?
      `).bind(
        providerName, apiUrl, apiKey, modelList[0],
        checkPrompt, expectedContains,
        timeoutMs, endpointId
      ).run();

      await env.DB.prepare(
        'UPDATE monitored_llm_endpoints SET name = ? WHERE api_url = ?'
      ).bind(providerName, apiUrl).run();

      if (info.changes === 0) {
        return createErrorResponse('Endpoint not found', '端点不存在', 404, corsHeaders);
      }

      // 后续模型：创建新端点
      const createdEndpoints = [];
      if (modelList.length > 1) {
        const addedAt = Math.floor(Date.now() / 1000);

        const maxOrderResult = await env.DB.prepare(
          'SELECT MAX(sort_order) as max_order FROM monitored_llm_endpoints'
        ).first();
        let nextSortOrder = (maxOrderResult?.max_order && typeof maxOrderResult.max_order === 'number')
          ? maxOrderResult.max_order + 1 : 0;

        const insertStmts = [];
        for (let i = 1; i < modelList.length; i++) {
          const newId = Math.random().toString(36).substring(2, 12);
          insertStmts.push(
            env.DB.prepare(`
              INSERT INTO monitored_llm_endpoints (id, name, api_url, api_key, model, check_prompt, expected_contains, timeout_ms, added_at, last_status, sort_order)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `).bind(newId, providerName, apiUrl, apiKey, modelList[i], checkPrompt, expectedContains, timeoutMs, addedAt, 'PENDING', nextSortOrder)
          );
          createdEndpoints.push({
            id: newId, name: providerName, api_url: apiUrl, api_key: apiKey,
            model: modelList[i], check_prompt: checkPrompt,
            expected_contains: expectedContains,
            timeout_ms: timeoutMs,
            added_at: addedAt, last_status: 'PENDING', sort_order: nextSortOrder
          });
          nextSortOrder++;
        }

        await env.DB.batch(insertStmts);

        // 为新创建的端点触发健康检查
        for (const ep of createdEndpoints) {
          if (ctx?.waitUntil) {
            ctx.waitUntil(checkLlmEndpointStatus(ep, env.DB, ctx));
          } else {
            checkLlmEndpointStatus(ep, env.DB, ctx).catch(() => {});
          }
        }
      }

      return createSuccessResponse({
        updated: endpointId,
        created: createdEndpoints.map(ep => ep.id),
        count: createdEndpoints.length,
        message: modelList.length > 1
          ? `更新成功，并新增 ${createdEndpoints.length} 个模型`
          : 'LLM端点更新成功'
      }, corsHeaders);
    } catch (error) {
      return handleDbError(error, corsHeaders, '更新LLM端点');
    }
  }

  // 删除LLM端点（管理员）
  if (path.match(/\/api\/admin\/llm-endpoints\/[^\/]+$/) && method === 'DELETE') {
    const user = await authenticateAdmin(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const endpointId = extractAndValidateServerId(path);
      if (!endpointId) {
        return createErrorResponse('Invalid endpoint ID', '无效的端点ID格式', 400, corsHeaders);
      }

      const url = new URL(request.url);
      const confirmed = url.searchParams.get('confirm') === 'true';
      if (!confirmed) {
        return createErrorResponse('Confirmation required', '删除操作需要确认，请添加 ?confirm=true 参数', 400, corsHeaders);
      }

      const info = await env.DB.prepare('DELETE FROM monitored_llm_endpoints WHERE id = ?').bind(endpointId).run();

      if (info.changes === 0) {
        return new Response(JSON.stringify({ error: 'Endpoint not found' }), {
          status: 404, headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      return createSuccessResponse({}, corsHeaders);
    } catch (error) {
      return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // LLM端点通知开关切换（管理员）
  if (path.match(/\/api\/admin\/llm-endpoints\/[^\/]+\/notifications$/) && method === 'PUT') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const pathParts = path.split('/');
      const endpointId = pathParts[pathParts.length - 2];
      const { enable_notifications } = await request.json();

      await env.DB.prepare('UPDATE monitored_llm_endpoints SET enable_notifications = ? WHERE id = ?')
        .bind(enable_notifications ? 1 : 0, endpointId).run();

      return createSuccessResponse({ id: endpointId, enable_notifications, message: '通知设置更新成功' }, corsHeaders);
    } catch (error) {
      return handleDbError(error, corsHeaders, '更新LLM端点通知设置');
    }
  }

  // LLM端点可见性切换（管理员）
  if (path.match(/\/api\/admin\/llm-endpoints\/[^\/]+\/visibility$/) && method === 'PUT') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const pathParts = path.split('/');
      const endpointId = pathParts[pathParts.length - 2];
      const { is_public } = await request.json();

      await env.DB.prepare('UPDATE monitored_llm_endpoints SET is_public = ? WHERE id = ?')
        .bind(is_public ? 1 : 0, endpointId).run();

      return createSuccessResponse({ id: endpointId, is_public, message: '可见性更新成功' }, corsHeaders);
    } catch (error) {
      return handleDbError(error, corsHeaders, '更新LLM端点可见性');
    }
  }

  // ==================== 公共API ====================

  // 获取所有监控站点状态（公开，支持管理员和游客模式）
  if (path === '/api/sites/status' && method === 'GET') {
    try {
      // 检查是否为管理员登录状态
      const user = await authenticateRequestOptional(request, env);
      const isAdmin = user !== null;

      let query = `
        SELECT id, name, url, last_checked, last_status, last_status_code, last_response_time_ms
        FROM monitored_sites
      `;
      if (!isAdmin) {
        query += ` WHERE is_public = 1`;
      }
      query += ` ORDER BY sort_order ASC NULLS LAST, name ASC, id ASC`;

      const { results } = await env.DB.prepare(query).all();
      const sites = results || [];

      // 为每个站点附加24小时历史数据
      const nowSeconds = Math.floor(Date.now() / 1000);
      const twentyFourHoursAgoSeconds = nowSeconds - (24 * 60 * 60);

      for (const site of sites) {
        try {
          const { results: historyResults } = await env.DB.prepare(`
            SELECT timestamp, status, status_code, response_time_ms
            FROM site_status_history
            WHERE site_id = ? AND timestamp >= ?
            ORDER BY timestamp DESC
          `).bind(site.id, twentyFourHoursAgoSeconds).all();

          site.history = historyResults || [];
        } catch (historyError) {
          site.history = [];
        }
      }

      return new Response(JSON.stringify({ sites }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
            if (error.message.includes('no such table')) {
                try {
          await env.DB.exec(D1_SCHEMAS.monitored_sites);
          return new Response(JSON.stringify({ sites: [] }), {
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
          });
        } catch (createError) {
                  }
      }
      return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // 获取所有LLM端点状态（公开，支持管理员和游客模式）
  if (path === '/api/llm-endpoints/status' && method === 'GET') {
    try {
      const user = await authenticateRequestOptional(request, env);
      const isAdmin = user !== null;

      let query = `
        SELECT id, name, api_url, model, last_checked, last_status, last_status_code,
               last_response_time_ms, last_output_preview
        FROM monitored_llm_endpoints
      `;
      if (!isAdmin) {
        query += ` WHERE is_public = 1`;
      }
      query += ` ORDER BY sort_order ASC NULLS LAST, model ASC`;

      const { results } = await env.DB.prepare(query).all();
      const endpoints = results || [];

      const nowSeconds = Math.floor(Date.now() / 1000);
      const twentyFourHoursAgoSeconds = nowSeconds - (24 * 60 * 60);

      for (const ep of endpoints) {
        try {
          const { results: historyResults } = await env.DB.prepare(`
            SELECT timestamp, status, status_code, response_time_ms, output_preview
            FROM llm_status_history
            WHERE endpoint_id = ? AND timestamp >= ?
            ORDER BY timestamp DESC
          `).bind(ep.id, twentyFourHoursAgoSeconds).all();

          ep.history = historyResults || [];
        } catch (historyError) {
          ep.history = [];
        }
      }

      return new Response(JSON.stringify({ endpoints }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
      if (error.message.includes('no such table')) {
        try {
          await env.DB.exec(D1_SCHEMAS.monitored_llm_endpoints);
          await env.DB.exec(D1_SCHEMAS.llm_status_history);
          return new Response(JSON.stringify({ endpoints: [] }), {
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
          });
        } catch (createError) {}
      }
      return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // ==================== VPS配置API ====================

  // 获取VPS上报间隔（公开，优化版本）
  if (path === '/api/admin/settings/vps-report-interval' && method === 'GET') {
    try {
      // 使用统一的缓存查询函数
      const interval = await getVpsReportInterval(env);

      return new Response(JSON.stringify({ interval }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
            // 任何错误都返回默认值，确保系统继续工作
      return new Response(JSON.stringify({ interval: 60 }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }
  }

  // 设置VPS上报间隔（管理员）
  if (path === '/api/admin/settings/vps-report-interval' && method === 'POST') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const { interval } = await request.json();
      if (typeof interval !== 'number' || interval <= 0 || !Number.isInteger(interval)) {
        return new Response(JSON.stringify({
          error: 'Invalid interval value. Must be a positive integer (seconds).'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      await env.DB.prepare('REPLACE INTO app_config (key, value) VALUES (?, ?)').bind(
        'vps_report_interval_seconds',
        interval.toString()
      ).run();

      // 清除相关缓存
      configCache.clearKey('monitoring_settings');
      vpsIntervalCache.value = null; // 清除VPS间隔缓存

      return new Response(JSON.stringify({ success: true, interval }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
            return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // 获取LLM检查频率（公开）
  if (path === '/api/admin/settings/llm-check-interval' && method === 'GET') {
    try {
      const row = await env.DB.prepare(
        "SELECT value FROM app_config WHERE key = 'llm_check_interval'"
      ).first();
      const interval = row?.value ? parseInt(row.value, 10) : 1;

      return new Response(JSON.stringify({ interval }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({ interval: 1 }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }
  }

  // 设置LLM检查频率（管理员）
  if (path === '/api/admin/settings/llm-check-interval' && method === 'POST') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const { interval } = await request.json();
      if (typeof interval !== 'number' || interval < 1 || !Number.isInteger(interval)) {
        return new Response(JSON.stringify({
          error: 'Invalid interval value. Must be a positive integer >= 1.'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      await env.DB.prepare('REPLACE INTO app_config (key, value) VALUES (?, ?)').bind(
        'llm_check_interval',
        interval.toString()
      ).run();

      return new Response(JSON.stringify({ success: true, interval }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
      return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // 获取高延迟阈值设置（管理员）
  if (path === '/api/admin/settings/high-latency-thresholds' && method === 'GET') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const thresholds = await getMonitoringThresholds(env.DB);
      return new Response(JSON.stringify(thresholds), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        websiteHighLatencyMs: WEBSITE_HIGH_LATENCY_MS,
        llmHighLatencyMs: LLM_HIGH_LATENCY_MS
      }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }
  }

  // 设置高延迟阈值（管理员）
  if (path === '/api/admin/settings/high-latency-thresholds' && method === 'POST') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const { websiteHighLatencyMs, llmHighLatencyMs } = await parseJsonSafely(request);
      const websiteThreshold = parsePositiveInteger(websiteHighLatencyMs, 0);
      const llmThreshold = parsePositiveInteger(llmHighLatencyMs, 0);

      if (!websiteThreshold || !llmThreshold) {
        return createErrorResponse('Invalid threshold', '阈值必须为大于 0 的整数', 400, corsHeaders);
      }
      if (websiteThreshold >= WEBSITE_CHECK_TIMEOUT_MS) {
        return createErrorResponse('Invalid threshold', `网站高延迟阈值必须小于 ${WEBSITE_CHECK_TIMEOUT_MS} ms`, 400, corsHeaders);
      }
      if (llmThreshold >= MAX_LLM_TIMEOUT_MS) {
        return createErrorResponse('Invalid threshold', `LLM 高延迟阈值必须小于 ${MAX_LLM_TIMEOUT_MS} ms`, 400, corsHeaders);
      }

      const effectiveMinTimeout = getEffectiveMinLlmTimeout(llmThreshold);
      await env.DB.batch([
        env.DB.prepare('REPLACE INTO app_config (key, value) VALUES (?, ?)').bind('website_high_latency_ms', String(websiteThreshold)),
        env.DB.prepare('REPLACE INTO app_config (key, value) VALUES (?, ?)').bind('llm_high_latency_ms', String(llmThreshold)),
        env.DB.prepare(
          'UPDATE monitored_llm_endpoints SET timeout_ms = ? WHERE timeout_ms IS NULL OR timeout_ms < ?'
        ).bind(effectiveMinTimeout, effectiveMinTimeout)
      ]);

      clearMonitoringSettingsCache();

      return createSuccessResponse({
        websiteHighLatencyMs: websiteThreshold,
        llmHighLatencyMs: llmThreshold,
        effectiveMinLlmTimeoutMs: effectiveMinTimeout
      }, corsHeaders);
    } catch (error) {
      return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }


  // ==================== Telegram配置API ====================

  // 获取Telegram设置（管理员）
  if (path === '/api/admin/telegram-settings' && method === 'GET') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const settings = await configCache.getTelegramConfig(env.DB);

      return new Response(JSON.stringify(
        settings || { bot_token: null, chat_id: null, enable_notifications: 0 }
      ), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
            if (error.message.includes('no such table')) {
        try {
          await env.DB.exec(D1_SCHEMAS.telegram_config);
          return new Response(JSON.stringify({
            bot_token: null,
            chat_id: null,
            enable_notifications: 0
          }), {
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
          });
        } catch (createError) {
                  }
      }
      return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // 设置Telegram配置（管理员）
  if (path === '/api/admin/telegram-settings' && method === 'POST') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const { bot_token, chat_id, enable_notifications } = await request.json();
      const updatedAt = Math.floor(Date.now() / 1000);
      const enableNotifValue = (enable_notifications === true || enable_notifications === 1) ? 1 : 0;

      await env.DB.prepare(`
        UPDATE telegram_config SET bot_token = ?, chat_id = ?, enable_notifications = ?, updated_at = ? WHERE id = 1
      `).bind(bot_token || null, chat_id || null, enableNotifValue, updatedAt).run();

      // 清除缓存，确保下次获取最新配置
      configCache.clearKey('telegram_config');

      // 发送测试通知（高优先级，立即发送）
      if (enableNotifValue === 1 && bot_token && chat_id) {
        const testMessage = "✅ Telegram通知已在此监控面板激活。这是一条测试消息。";
        if (ctx?.waitUntil) {
          ctx.waitUntil(sendTelegramNotificationOptimized(env.DB, testMessage, 'high'));
        } else {
                    sendTelegramNotificationOptimized(env.DB, testMessage, 'high').catch(e => {
            // 静默处理测试通知错误
          });
        }
      }

      return createSuccessResponse({}, corsHeaders);
    } catch (error) {
            return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // ==================== ntfy通知设置API ====================

  // 获取ntfy配置（管理员）
  if (path === '/api/admin/ntfy-settings' && method === 'GET') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const settings = await configCache.getNtfyConfig(env.DB);

      return new Response(JSON.stringify(
        settings || { topic: null, server_url: 'https://ntfy.sh', enable_notifications: 0 }
      ), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
            if (error.message.includes('no such table')) {
        try {
          await env.DB.exec(D1_SCHEMAS.ntfy_config);
          return new Response(JSON.stringify({
            topic: null,
            server_url: 'https://ntfy.sh',
            enable_notifications: 0
          }), {
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
          });
        } catch (createError) {
                  }
      }
      return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // 设置ntfy配置（管理员）
  if (path === '/api/admin/ntfy-settings' && method === 'POST') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const { topic, server_url, enable_notifications } = await request.json();
      const updatedAt = Math.floor(Date.now() / 1000);
      const enableNotifValue = (enable_notifications === true || enable_notifications === 1) ? 1 : 0;
      const serverUrl = (server_url && server_url.trim()) ? server_url.trim() : 'https://ntfy.sh';

      await env.DB.prepare(`
        UPDATE ntfy_config SET topic = ?, server_url = ?, enable_notifications = ?, updated_at = ? WHERE id = 1
      `).bind(topic || null, serverUrl, enableNotifValue, updatedAt).run();

      // 清除缓存
      configCache.clearKey('ntfy_config');

      // 发送测试通知
      if (enableNotifValue === 1 && topic) {
        const testMessage = "✅ ntfy通知已在此监控面板激活。这是一条测试消息。";
        if (ctx?.waitUntil) {
          ctx.waitUntil(sendNtfyNotification(env.DB, testMessage, 'high'));
        } else {
                    sendNtfyNotification(env.DB, testMessage, 'high').catch(e => {
            // 静默处理测试通知错误
          });
        }
      }

      return createSuccessResponse({}, corsHeaders);
    } catch (error) {
            return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }

  // ==================== 背景设置API ====================

  // 获取背景设置（公开API - 所有用户可访问）
  if (path === '/api/background-settings' && method === 'GET') {
    try {
      // 查询三个背景配置项
      const { results } = await env.DB.prepare(`
        SELECT key, value FROM app_config
        WHERE key IN ('custom_background_enabled', 'custom_background_url', 'page_opacity')
      `).all();

      // 转换为对象格式
      const settings = {
        enabled: false,
        url: '',
        opacity: 80
      };

      results.forEach(row => {
        switch (row.key) {
          case 'custom_background_enabled':
            settings.enabled = row.value === 'true';
            break;
          case 'custom_background_url':
            settings.url = row.value || '';
            break;
          case 'page_opacity':
            settings.opacity = parseInt(row.value, 10) || 80;
            break;
        }
      });

      return new Response(JSON.stringify(settings), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
            return new Response(JSON.stringify({
        enabled: false,
        url: '',
        opacity: 80
      }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }
  }



  // 设置背景配置（管理员）
  if (path === '/api/admin/background-settings' && method === 'POST') {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse('Unauthorized', '需要管理员权限', 401, corsHeaders);
    }

    try {
      const { enabled, url, opacity } = await request.json();

      // 验证输入参数
      if (typeof enabled !== 'boolean') {
        return new Response(JSON.stringify({
          error: 'Invalid enabled value',
          message: 'enabled必须是布尔值'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      if (enabled && url) {
        if (typeof url !== 'string' || !url.startsWith('https://')) {
          return new Response(JSON.stringify({
            error: 'Invalid URL format',
            message: '背景图片URL必须以https://开头'
          }), {
            status: 400,
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
          });
        }
      }

      if (typeof opacity !== 'number' || opacity < 0 || opacity > 100) {
        return new Response(JSON.stringify({
          error: 'Invalid opacity value',
          message: '透明度必须是0-100之间的数字'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      // 更新配置到数据库
      await env.DB.batch([
        env.DB.prepare('REPLACE INTO app_config (key, value) VALUES (?, ?)').bind(
          'custom_background_enabled',
          enabled.toString()
        ),
        env.DB.prepare('REPLACE INTO app_config (key, value) VALUES (?, ?)').bind(
          'custom_background_url',
          url || ''
        ),
        env.DB.prepare('REPLACE INTO app_config (key, value) VALUES (?, ?)').bind(
          'page_opacity',
          opacity.toString()
        )
      ]);

      // 清除监控设置缓存（背景设置也在app_config表中）
      configCache.clearKey('monitoring_settings');

      return new Response(JSON.stringify({
        success: true,
        settings: { enabled, url: url || '', opacity }
      }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
            return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }




  // 获取监控站点24小时历史状态（公开）
  if (path.match(/\/api\/sites\/[^\/]+\/history$/) && method === 'GET') {
    try {
      const siteId = path.split('/')[3];
      const nowSeconds = Math.floor(Date.now() / 1000);
      const twentyFourHoursAgoSeconds = nowSeconds - (24 * 60 * 60);

      const { results } = await env.DB.prepare(`
        SELECT timestamp, status, status_code, response_time_ms
        FROM site_status_history
        WHERE site_id = ? AND timestamp >= ?
        ORDER BY timestamp DESC
      `).bind(siteId, twentyFourHoursAgoSeconds).all();

      return new Response(JSON.stringify({ history: results || [] }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
            if (error.message.includes('no such table')) {
                try {
          await env.DB.exec(D1_SCHEMAS.site_status_history);
          return new Response(JSON.stringify({ history: [] }), {
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
          });
        } catch (createError) {
                  }
      }
      return createErrorResponse('Internal server error', error.message, 500, corsHeaders);
    }
  }


  // 未找到匹配的API路由
  return new Response(JSON.stringify({ error: 'API endpoint not found' }), {
    status: 404,
    headers: { 'Content-Type': 'application/json', ...corsHeaders }
  });
}


// ==================== 网站状态检查（优化版） ====================

// 优化版网站状态检查 - 减少超时时间，使用缓存
async function checkWebsiteStatusOptimized(site, db, ctx) {
  const { id, url, name } = site;
  const startTime = Date.now();
  let newStatus = 'PENDING';
  let newStatusCode = null;
  let newResponseTime = null;
  const { websiteHighLatencyMs } = await getMonitoringThresholds(db);

  // 获取当前状态
  let previousStatus = 'PENDING';
  let siteLastNotifiedDownAt = null;

  try {
    const siteDetailsResult = await db.prepare(
      'SELECT last_status, last_notified_down_at FROM monitored_sites WHERE id = ?'
    ).bind(id).first();

    if (siteDetailsResult) {
      previousStatus = siteDetailsResult.last_status || 'PENDING';
      siteLastNotifiedDownAt = siteDetailsResult.last_notified_down_at;
    }
  } catch (error) {
        // 静默处理错误
    }

  const NOTIFICATION_INTERVAL_SECONDS = 1 * 60 * 60; // 1小时

  try {
    // 超时时间提高到 WEBSITE_CHECK_TIMEOUT_MS 以容纳高延迟判定（>WEBSITE_HIGH_LATENCY_MS 但未超时）
    const response = await fetch(url, {
      method: 'HEAD',
      redirect: 'follow',
      signal: AbortSignal.timeout(WEBSITE_CHECK_TIMEOUT_MS)
    });

    newResponseTime = Date.now() - startTime;
    newStatusCode = response.status;

    if (response.ok || (response.status >= 300 && response.status < 500)) {
      // 可用：响应过慢则单独标记为 HIGH_LATENCY，仅展示不触发故障通知
      newStatus = newResponseTime > websiteHighLatencyMs ? 'HIGH_LATENCY' : 'UP';
    } else {
      newStatus = 'DOWN';
    }
  } catch (error) {
    newResponseTime = Date.now() - startTime;
    if (error.name === 'TimeoutError') {
      newStatus = 'TIMEOUT';
    } else {
      newStatus = 'ERROR';
    }
  }

  const checkTime = Math.floor(Date.now() / 1000);
  const siteDisplayName = name || url;
  let newSiteLastNotifiedDownAt = siteLastNotifiedDownAt;

  // 通知逻辑保持不变
  if (['DOWN', 'TIMEOUT', 'ERROR'].includes(newStatus)) {
    const isFirstTimeDown = !['DOWN', 'TIMEOUT', 'ERROR'].includes(previousStatus);
    if (isFirstTimeDown) {
      const message = `🔴 网站故障: *${siteDisplayName}* 当前状态 ${newStatus.toLowerCase()} (状态码: ${newStatusCode || '无'}).\n网址: ${url}`;
      ctx.waitUntil(sendNotifications(db, message));
      newSiteLastNotifiedDownAt = checkTime;
    } else {
      const shouldResend = siteLastNotifiedDownAt === null || (checkTime - siteLastNotifiedDownAt > NOTIFICATION_INTERVAL_SECONDS);
      if (shouldResend) {
        const message = `🔴 网站持续故障: *${siteDisplayName}* 状态 ${newStatus.toLowerCase()} (状态码: ${newStatusCode || '无'}).\n网址: ${url}`;
        ctx.waitUntil(sendNotifications(db, message));
        newSiteLastNotifiedDownAt = checkTime;
      }
    }
  } else if ((newStatus === 'UP' || newStatus === 'HIGH_LATENCY') && ['DOWN', 'TIMEOUT', 'ERROR'].includes(previousStatus)) {
    const message = `✅ 网站恢复: *${siteDisplayName}* 已恢复在线!\n网址: ${url}`;
    ctx.waitUntil(sendNotifications(db, message));
    newSiteLastNotifiedDownAt = null;
  }
  // 批量更新数据库
  try {
    await db.batch([
      db.prepare('UPDATE monitored_sites SET last_checked = ?, last_status = ?, last_status_code = ?, last_response_time_ms = ?, last_notified_down_at = ? WHERE id = ?')
        .bind(checkTime, newStatus, newStatusCode, newResponseTime, newSiteLastNotifiedDownAt, id),
      db.prepare('INSERT INTO site_status_history (site_id, timestamp, status, status_code, response_time_ms) VALUES (?, ?, ?, ?, ?)')
        .bind(id, checkTime, newStatus, newStatusCode, newResponseTime)
    ]);
  } catch (dbError) {
    // 静默处理数据库更新错误
  }
}

// ==================== LLM端点健康检查 ====================

async function checkLlmEndpointStatus(endpoint, db, ctx) {
  const { id, name, api_url, api_key, model, check_prompt, expected_contains, timeout_ms } = endpoint;
  const startTime = Date.now();
  let newStatus = 'PENDING';
  let newStatusCode = null;
  let newResponseTime = null;
  let outputPreview = null;

  const displayName = name || model;

  // 获取当前状态和通知设置
  let previousStatus = 'PENDING';
  let lastNotifiedDownAt = null;
  let enableNotifications = 1;

  try {
    const details = await db.prepare(
      'SELECT last_status, last_notified_down_at, enable_notifications FROM monitored_llm_endpoints WHERE id = ?'
    ).bind(id).first();

    if (details) {
      previousStatus = details.last_status || 'PENDING';
      lastNotifiedDownAt = details.last_notified_down_at;
      enableNotifications = details.enable_notifications !== 0 ? 1 : 0;
    }
  } catch (error) {
    // 静默处理
  }

  const NOTIFICATION_INTERVAL_SECONDS = 1 * 60 * 60; // 1小时
  const { llmHighLatencyMs } = await getMonitoringThresholds(db);
  const effectiveTimeout = normalizeLlmTimeout(timeout_ms, llmHighLatencyMs);

  try {
    const headers = { 'Content-Type': 'application/json' };
    if (api_key) {
      headers['Authorization'] = `Bearer ${api_key}`;
    }

    const prompt = check_prompt || 'Say hello in one word';
    const body = JSON.stringify({
      model: model,
      messages: [{ role: 'user', content: prompt }],
      max_tokens: 50,
      stream: false
    });

    const response = await fetch(api_url, {
      method: 'POST',
      headers: headers,
      body: body,
      redirect: 'follow',
      signal: AbortSignal.timeout(effectiveTimeout)
    });

    newResponseTime = Date.now() - startTime;
    newStatusCode = response.status;

    if (response.ok) {
      try {
        const data = await response.json();
        // 兼容 OpenAI 格式和其他兼容格式
        const content = data?.choices?.[0]?.message?.content
          || data?.content?.[0]?.text
          || data?.output_text
          || null;

        if (content) {
          outputPreview = content.substring(0, 200);

          // 检查期望内容
          if (expected_contains) {
            const patterns = expected_contains.split(',').map(p => p.trim().toLowerCase()).filter(Boolean);
            const contentLower = content.toLowerCase();
            const matched = patterns.some(p => contentLower.includes(p));
            // 可用：响应过慢则单独标记为 HIGH_LATENCY，仅展示不触发故障通知
            newStatus = matched ? (newResponseTime > llmHighLatencyMs ? 'HIGH_LATENCY' : 'UP') : 'DOWN';
          } else {
            newStatus = newResponseTime > llmHighLatencyMs ? 'HIGH_LATENCY' : 'UP';
          }
        } else {
          // API 返回 200 但无有效内容
          outputPreview = JSON.stringify(data).substring(0, 200);
          newStatus = 'DOWN';
        }
      } catch (parseError) {
        outputPreview = 'Response parse error';
        newStatus = 'DOWN';
      }
    } else {
      newStatus = 'DOWN';
      try {
        const errBody = await response.text();
        outputPreview = errBody.substring(0, 200);
      } catch {}
    }
  } catch (error) {
    newResponseTime = Date.now() - startTime;
    if (error.name === 'TimeoutError' || error.name === 'AbortError') {
      newStatus = 'TIMEOUT';
      outputPreview = 'Request timed out';
    } else {
      newStatus = 'ERROR';
      outputPreview = error.message?.substring(0, 200) || 'Connection error';
    }
  }

  const checkTime = Math.floor(Date.now() / 1000);
  let newLastNotifiedDownAt = lastNotifiedDownAt;

  // 通知逻辑（仅在启用通知时发送通知，但状态追踪独立于通知开关）
  if (['DOWN', 'TIMEOUT', 'ERROR'].includes(newStatus)) {
    const isFirstTimeDown = !['DOWN', 'TIMEOUT', 'ERROR'].includes(previousStatus);
    if (isFirstTimeDown) {
      if (enableNotifications) {
        const message = `🔴 LLM端点故障: *${displayName}* 当前状态 ${newStatus.toLowerCase()} (状态码: ${newStatusCode || '无'}).\n模型: ${model}\nURL: ${api_url}`;
        ctx.waitUntil(sendNotifications(db, message));
      }
      newLastNotifiedDownAt = checkTime;
    } else {
      const shouldResend = lastNotifiedDownAt === null || (checkTime - lastNotifiedDownAt > NOTIFICATION_INTERVAL_SECONDS);
      if (shouldResend) {
        if (enableNotifications) {
          const message = `🔴 LLM端点持续故障: *${displayName}* 状态 ${newStatus.toLowerCase()} (状态码: ${newStatusCode || '无'}).\n模型: ${model}\nURL: ${api_url}`;
          ctx.waitUntil(sendNotifications(db, message));
        }
        newLastNotifiedDownAt = checkTime;
      }
    }
  } else if ((newStatus === 'UP' || newStatus === 'HIGH_LATENCY') && ['DOWN', 'TIMEOUT', 'ERROR'].includes(previousStatus)) {
    if (enableNotifications) {
      const message = `✅ LLM端点恢复: *${displayName}* 已恢复在线!\n模型: ${model}\nURL: ${api_url}`;
      ctx.waitUntil(sendNotifications(db, message));
    }
    newLastNotifiedDownAt = null;
  }

  // 更新数据库
  try {
    await db.batch([
      db.prepare('UPDATE monitored_llm_endpoints SET last_checked = ?, last_status = ?, last_status_code = ?, last_response_time_ms = ?, last_output_preview = ?, last_notified_down_at = ? WHERE id = ?')
        .bind(checkTime, newStatus, newStatusCode, newResponseTime, outputPreview, newLastNotifiedDownAt, id),
      db.prepare('INSERT INTO llm_status_history (endpoint_id, timestamp, status, status_code, response_time_ms, output_preview) VALUES (?, ?, ?, ?, ?, ?)')
        .bind(id, checkTime, newStatus, newStatusCode, newResponseTime, outputPreview)
    ]);
  } catch (dbError) {
    // 静默处理数据库更新错误
  }
}

// 简化版VPS离线提醒检查 - 只负责持续离线提醒
async function checkVpsOfflineReminder(env, ctx) {
  try {
    const telegramConfig = await configCache.getTelegramConfig(env.DB);

    if (!telegramConfig?.enable_notifications || !telegramConfig.bot_token || !telegramConfig.chat_id) {
      return;
    }

    const currentTime = Math.floor(Date.now() / 1000);
    const offlineThreshold = 5 * 60; // 5分钟
    const reminderInterval = 60 * 60; // 1小时

    // 查询持续离线的VPS（已有离线记录且仍然离线）
    const { results: offlineServers } = await env.DB.prepare(`
      SELECT s.id, s.name, s.last_notified_down_at, m.timestamp as last_report
      FROM servers s
      LEFT JOIN metrics m ON s.id = m.server_id
      WHERE s.last_notified_down_at IS NOT NULL
        AND (m.timestamp IS NULL OR m.timestamp < ?)
        AND s.last_notified_down_at < ?
    `).bind(currentTime - offlineThreshold, currentTime - reminderInterval).all();

    for (const server of offlineServers) {
      const serverDisplayName = server.name || server.id;
      const offlineHours = Math.floor((currentTime - server.last_notified_down_at) / 3600);

      const message = `🔴 VPS持续离线: 服务器 *${serverDisplayName}* 已离线${offlineHours}小时（每小时提醒）`;
      ctx.waitUntil(sendNotifications(env.DB, message));

      // 更新最后通知时间
      ctx.waitUntil(env.DB.prepare('UPDATE servers SET last_notified_down_at = ? WHERE id = ?')
        .bind(currentTime, server.id).run());
    }

  } catch (error) {
    // 静默处理VPS离线提醒错误
  }
}

// 简化版Telegram通知 - 直接发送
async function sendTelegramNotificationOptimized(db, message, priority = 'normal') {
  try {
    const telegramConfig = await configCache.getTelegramConfig(db);

    if (!telegramConfig?.enable_notifications || !telegramConfig.bot_token || !telegramConfig.chat_id) {
      return;
    }

    const telegramUrl = `https://api.telegram.org/bot${telegramConfig.bot_token}/sendMessage`;
    const payload = {
      chat_id: telegramConfig.chat_id,
      text: message,
      parse_mode: 'Markdown'
    };

    const response = await fetch(telegramUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

  } catch (error) {
    // 静默处理Telegram通知错误
  }
}

// ntfy通知发送函数
async function sendNtfyNotification(db, message, priority = 'normal') {
  try {
    const ntfyConfig = await configCache.getNtfyConfig(db);

    if (!ntfyConfig?.enable_notifications || !ntfyConfig.topic) {
      return;
    }

    const serverUrl = ntfyConfig.server_url || 'https://ntfy.sh';
    const ntfyUrl = `${serverUrl.replace(/\/+$/, '')}/${ntfyConfig.topic}`;

    // 将Markdown格式转换为纯文本（ntfy不支持完整Markdown，但支持部分格式）
    const cleanMessage = message.replace(/\*([^*]+)\*/g, '$1');

    // 优先级映射：high->5(urgent), normal->3(default)
    const ntfyPriority = priority === 'high' ? 5 : 3;

    const response = await fetch(ntfyUrl, {
      method: 'POST',
      body: cleanMessage,
      headers: {
        'Title': 'VPS监控面板',
        'Priority': String(ntfyPriority),
        'Tags': 'computer'
      }
    });

  } catch (error) {
    // 静默处理ntfy通知错误
  }
}

// 统一通知发送 - 发送到所有已启用的通知渠道
async function sendNotifications(db, message, priority = 'normal') {
  // 发送Telegram通知（如果已启用）
  const tgPromise = sendTelegramNotificationOptimized(db, message, priority).catch(() => {});
  // 发送ntfy通知（如果已启用）
  const ntfyPromise = sendNtfyNotification(db, message, priority).catch(() => {});
  await Promise.all([tgPromise, ntfyPromise]);
}

// ==================== 数据库维护系统 ====================

// 简洁的数据库维护函数
async function performDatabaseMaintenance(db) {
  const thirtyDaysAgo = Math.floor(Date.now() / 1000) - (30 * 24 * 60 * 60);

  try {
    // 清理30天前的网站状态历史
    await db.prepare(
      'DELETE FROM site_status_history WHERE timestamp < ?'
    ).bind(thirtyDaysAgo).run();

    // 清理30天前的LLM状态历史
    try {
      await db.prepare(
        'DELETE FROM llm_status_history WHERE timestamp < ?'
      ).bind(thirtyDaysAgo).run();
    } catch (e) {
      // 表可能不存在，静默处理
    }

    // 清理JWT缓存
    cleanupJWTCache();

  } catch (error) {
    // 静默处理数据库维护错误
  }
}

// ==================== 主函数导出 ====================

export default {
  async fetch(request, env, ctx) {
    // 优化：仅在必要时初始化数据库表
    if (!dbInitialized) {
      try {
        await ensureTablesExist(env.DB, env);
        dbInitialized = true;
      } catch (error) {
        // 静默处理数据库初始化失败
      }
    }

    // 定时刷新VPS批量数据（在每个请求中检查）
    scheduleVpsBatchFlush(env, ctx);

    const url = new URL(request.url);
    const path = url.pathname;

    // API请求处理
    if (path.startsWith('/api/')) {
      return handleApiRequest(request, env, ctx);
    }

    // 安装脚本处理
    if (path === '/install.sh') {
      return handleInstallScript(request, url, env);
    }

    // 前端静态文件处理
    return handleFrontendRequest(request, path);
  },

  async scheduled(event, env, ctx) {
    taskCounter++;

    ctx.waitUntil(
      (async () => {
        try {
          // 智能数据库初始化 - 仅在必要时执行
          if (!dbInitialized || taskCounter % 10 === 1) {
            await ensureTablesExist(env.DB, env);
            dbInitialized = true;
          }

          // ==================== 网站监控部分 ====================
          const { results: sitesToCheck } = await env.DB.prepare(
            'SELECT id, url, name FROM monitored_sites'
          ).all();

          if (sitesToCheck?.length > 0) {
            // 限制并发数量为5个，优化资源使用
            const siteConcurrencyLimit = 5;
            const sitePromises = [];

            for (const site of sitesToCheck) {
              sitePromises.push(checkWebsiteStatusOptimized(site, env.DB, ctx));
              if (sitePromises.length >= siteConcurrencyLimit) {
                await Promise.all(sitePromises);
                sitePromises.length = 0;
              }
            }

            if (sitePromises.length > 0) {
              await Promise.all(sitePromises);
            }
          }

          // ==================== LLM端点监控部分 ====================
          // 支持独立频率控制：通过 app_config 的 llm_check_interval 设置
          // 值为1=每次cron都检查，2=隔一次检查，3=每三次检查一次，以此类推
          try {
            let llmCheckInterval = 1;
            try {
              const intervalRow = await env.DB.prepare(
                "SELECT value FROM app_config WHERE key = 'llm_check_interval'"
              ).first();
              if (intervalRow?.value) {
                llmCheckInterval = Math.max(1, parseInt(intervalRow.value, 10) || 1);
              }
            } catch (configErr) {
              // 配置读取失败，使用默认值1
            }

            if (taskCounter % llmCheckInterval === 0) {
              const { results: llmEndpoints } = await env.DB.prepare(
                'SELECT id, name, api_url, api_key, model, check_prompt, expected_contains, timeout_ms FROM monitored_llm_endpoints'
              ).all();

              if (llmEndpoints?.length > 0) {
                const llmConcurrencyLimit = 3;
                const llmPromises = [];

                for (const endpoint of llmEndpoints) {
                  llmPromises.push(checkLlmEndpointStatus(endpoint, env.DB, ctx));
                  if (llmPromises.length >= llmConcurrencyLimit) {
                    await Promise.all(llmPromises);
                    llmPromises.length = 0;
                  }
                }

                if (llmPromises.length > 0) {
                  await Promise.all(llmPromises);
                }
              }
            }
          } catch (llmError) {
            // 静默处理LLM监控错误（表可能不存在）
          }

          // ==================== VPS离线提醒检查 ====================
          // 每小时执行一次，发送持续离线提醒
          await checkVpsOfflineReminder(env, ctx);

          // ==================== 数据库维护检查 ====================
          // 每天执行一次数据库维护
          if (taskCounter % 1440 === 0) {
            await performDatabaseMaintenance(env.DB);
          }

        } catch (error) {
          // 静默处理定时任务错误
        }
      })()
    );
  }
};


// ==================== 工具函数 ====================

// HTTP/HTTPS URL验证
function isValidHttpUrl(string) {
  try {
    const url = new URL(string);
    return ['http:', 'https:'].includes(url.protocol);
  } catch {
    return false;
  }
}


// ==================== 处理函数 ====================

// 安装脚本处理
async function handleInstallScript(request, url, env) {
  const baseUrl = url.origin;
  let vpsReportInterval = '60'; // 默认值

  try {
    // 确保app_config表存在
    if (D1_SCHEMAS?.app_config) {
      await env.DB.exec(D1_SCHEMAS.app_config);
    } else {
          }

    // 使用统一的缓存查询函数
    const interval = await getVpsReportInterval(env);
    vpsReportInterval = interval.toString();
  } catch (e) {
        // 使用默认值
  }

  const script = `#!/bin/bash
# VPS监控脚本 - 安装程序

# 默认值
API_KEY=""
SERVER_ID=""
WORKER_URL="${baseUrl}"
INSTALL_DIR="/opt/vps-monitor"
SERVICE_NAME="vps-monitor"

# 解析参数
while [[ $# -gt 0 ]]; do
  case $1 in
    -k|--key)
      API_KEY="$2"
      shift 2
      ;;
    -s|--server)
      SERVER_ID="$2"
      shift 2
      ;;
    -u|--url)
      WORKER_URL="$2"
      shift 2
      ;;
    -d|--dir)
      INSTALL_DIR="$2"
      shift 2
      ;;
    *)
      echo "未知参数: $1"
      exit 1
      ;;
  esac
done

# 检查必要参数
if [ -z "$API_KEY" ] || [ -z "$SERVER_ID" ]; then
  echo "错误: API密钥和服务器ID是必需的"
  echo "用法: $0 -k API_KEY -s SERVER_ID [-u WORKER_URL] [-d INSTALL_DIR]"
  exit 1
fi

# 检查权限
if [ "$(id -u)" -ne 0 ]; then
  echo "错误: 此脚本需要root权限"
  exit 1
fi

echo "=== VPS监控脚本安装程序 ==="
echo "安装目录: $INSTALL_DIR"
echo "Worker URL: $WORKER_URL"

# 创建安装目录
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR" || exit 1

# 创建监控脚本
cat > "$INSTALL_DIR/monitor.sh" << 'EOF'
#!/bin/bash

# 配置
API_KEY="__API_KEY__"
SERVER_ID="__SERVER_ID__"
WORKER_URL="__WORKER_URL__"
INTERVAL=${vpsReportInterval}  # 上报间隔（秒）

# 日志函数
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# 获取CPU使用率
get_cpu_usage() {
  cpu_usage=$(top -bn1 | grep "Cpu(s)" | sed "s/.*, *\\([0-9.]*\\)%* id.*/\\1/" | awk '{print 100 - $1}')
  cpu_load=$(cat /proc/loadavg | awk '{print $1","$2","$3}')
  echo "{\"usage_percent\":$cpu_usage,\"load_avg\":[$cpu_load]}"
}

# 获取内存使用情况
get_memory_usage() {
  total=$(free -k | grep Mem | awk '{print $2}')
  used=$(free -k | grep Mem | awk '{print $3}')
  free=$(free -k | grep Mem | awk '{print $4}')
  usage_percent=$(echo "scale=1; $used * 100 / $total" | bc)
  echo "{\"total\":$total,\"used\":$used,\"free\":$free,\"usage_percent\":$usage_percent}"
}

# 获取硬盘使用情况
get_disk_usage() {
  disk_info=$(df -k / | tail -1)
  total=$(echo "$disk_info" | awk '{print $2 / 1024 / 1024}')
  used=$(echo "$disk_info" | awk '{print $3 / 1024 / 1024}')
  free=$(echo "$disk_info" | awk '{print $4 / 1024 / 1024}')
  usage_percent=$(echo "$disk_info" | awk '{print $5}' | tr -d '%')
  echo "{\"total\":$total,\"used\":$used,\"free\":$free,\"usage_percent\":$usage_percent}"
}

# 获取网络使用情况
get_network_usage() {
  # 检查是否安装了ifstat
  if ! command -v ifstat &> /dev/null; then
    log "ifstat未安装，无法获取网络速度"
    echo "{\"upload_speed\":0,\"download_speed\":0,\"total_upload\":0,\"total_download\":0}"
    return
  fi

  # 获取网络接口
  interface=$(ip route | grep default | awk '{print $5}')

  # 获取网络速度（KB/s）
  network_speed=$(ifstat -i "$interface" 1 1 | tail -1)
  download_speed=$(echo "$network_speed" | awk '{print $1 * 1024}')
  upload_speed=$(echo "$network_speed" | awk '{print $2 * 1024}')

  # 获取总流量
  rx_bytes=$(cat /proc/net/dev | grep "$interface" | awk '{print $2}')
  tx_bytes=$(cat /proc/net/dev | grep "$interface" | awk '{print $10}')

  echo "{\"upload_speed\":$upload_speed,\"download_speed\":$download_speed,\"total_upload\":$tx_bytes,\"total_download\":$rx_bytes}"
}

# 上报数据
report_metrics() {
  timestamp=$(date +%s)
  cpu=$(get_cpu_usage)
  memory=$(get_memory_usage)
  disk=$(get_disk_usage)
  network=$(get_network_usage)

  data="{\"timestamp\":$timestamp,\"cpu\":$cpu,\"memory\":$memory,\"disk\":$disk,\"network\":$network}"

  log "正在上报数据..."
  log "API密钥: $API_KEY"
  log "服务器ID: $SERVER_ID"
  log "Worker URL: $WORKER_URL"

  response=$(curl -s -X POST "$WORKER_URL/api/report/$SERVER_ID" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: $API_KEY" \
    -d "$data")

  if [[ "$response" == *"success"* ]]; then
    log "数据上报成功"
  else
    log "数据上报失败: $response"
  fi
}

# 安装依赖
install_dependencies() {
  log "检查并安装依赖..."

  # 检测包管理器
  if command -v apt-get &> /dev/null; then
    PKG_MANAGER="apt-get"
  elif command -v yum &> /dev/null; then
    PKG_MANAGER="yum"
  else
    log "不支持的系统，无法自动安装依赖"
    return 1
  fi

  # 安装依赖
  $PKG_MANAGER update -y
  $PKG_MANAGER install -y bc curl ifstat

  log "依赖安装完成"
  return 0
}

# 主函数
main() {
  log "VPS监控脚本启动"

  # 安装依赖
  install_dependencies

  # 主循环
  while true; do
    report_metrics
    sleep $INTERVAL
  done
}

# 启动主函数
main
EOF

# 替换配置
sed -i "s|__API_KEY__|$API_KEY|g" "$INSTALL_DIR/monitor.sh"
sed -i "s|__SERVER_ID__|$SERVER_ID|g" "$INSTALL_DIR/monitor.sh"
sed -i "s|__WORKER_URL__|$WORKER_URL|g" "$INSTALL_DIR/monitor.sh"
# This line ensures the INTERVAL placeholder is replaced with the fetched value.
sed -i "s|^INTERVAL=.*|INTERVAL=${vpsReportInterval}|g" "$INSTALL_DIR/monitor.sh"

# 设置执行权限
chmod +x "$INSTALL_DIR/monitor.sh"

# 创建systemd服务
cat > "/etc/systemd/system/$SERVICE_NAME.service" << EOF
[Unit]
Description=VPS Monitor Service
After=network.target

[Service]
ExecStart=$INSTALL_DIR/monitor.sh
Restart=always
User=root
Group=root
Environment=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

[Install]
WantedBy=multi-user.target
EOF

# 启动服务
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl start "$SERVICE_NAME"

echo "=== 安装完成 ==="
echo "服务已启动并设置为开机自启"
echo "查看服务状态: systemctl status $SERVICE_NAME"
echo "查看服务日志: journalctl -u $SERVICE_NAME -f"
`;

  return new Response(script, {
    headers: {
      'Content-Type': 'text/plain',
      'Content-Disposition': 'attachment; filename="install.sh"'
    }
  });
}

// 前端请求处理
function handleFrontendRequest(request, path) {
  const routes = {
    '/': () => new Response(getIndexHtml(), { headers: { 'Content-Type': 'text/html' } }),
    '': () => new Response(getIndexHtml(), { headers: { 'Content-Type': 'text/html' } }),
    '/login': () => new Response(getLoginHtml(), { headers: { 'Content-Type': 'text/html' } }),
    '/login.html': () => new Response(getLoginHtml(), { headers: { 'Content-Type': 'text/html' } }),
    '/admin': () => new Response(getAdminHtml(), { headers: { 'Content-Type': 'text/html' } }),
    '/admin.html': () => new Response(getAdminHtml(), { headers: { 'Content-Type': 'text/html' } }),
    '/css/style.css': () => new Response(getStyleCss(), { headers: { 'Content-Type': 'text/css' } }),
    '/js/main.js': () => new Response(getMainJs(), { headers: { 'Content-Type': 'application/javascript' } }),
    '/js/login.js': () => new Response(getLoginJs(), { headers: { 'Content-Type': 'application/javascript' } }),
    '/js/admin.js': () => new Response(getAdminJs(), { headers: { 'Content-Type': 'application/javascript' } }),
    '/favicon.svg': () => new Response(getFaviconSvg(), { headers: { 'Content-Type': 'image/svg+xml' } })
  };

  const handler = routes[path];
  if (handler) {
    return handler();
  }

  // 404页面
  return new Response('Not Found', {
    status: 404,
    headers: { 'Content-Type': 'text/plain' }
  });
}

// ==================== 前端代码 ====================

// 主页HTML
function getIndexHtml() {
  return `<!DOCTYPE html>
<html lang="zh-CN" data-bs-theme="light">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>VPS监控面板</title>
    <link rel="icon" type="image/svg+xml" href="/favicon.svg">
    <script>
        // 立即设置主题，避免闪烁
        (function() {
            const theme = localStorage.getItem('vps-monitor-theme') || 'light';
            document.documentElement.setAttribute('data-bs-theme', theme);
        })();
    </script>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-T3c6CoIi6uLrA9TneNEoa7RxnatzjcDSCmG1MXxSR1GAsXEV/Dwwykc2MPK8M2HN" crossorigin="anonymous">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/font/bootstrap-icons.css" rel="stylesheet" integrity="sha384-4LISF5TTJX/fLmGSxO53rV4miRxdg84mZsxmO8Rx5jGtp/LbrixFETvWa5a6sESd" crossorigin="anonymous">
    <link href="/css/style.css" rel="stylesheet">
    <style>
        .server-row {
            cursor: pointer; /* Indicate clickable rows */
        }
        .site-name-link {
            text-decoration: none;
            color: inherit;
            font-weight: 500;
        }
        .site-name-link:hover {
            text-decoration: underline;
            color: #0d6efd;
        }
        .server-details-row {
            /* display: none; /* Initially hidden - controlled by JS */ */
        }
        .server-details-row td {
            padding: 1rem;
            background-color: rgba(248, 249, 250, var(--page-opacity, 0.8)); /* Light background for details with transparency */
        }
        .server-details-content {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
        }
        .detail-item {
            background-color: rgba(233, 236, 239, var(--page-opacity, 0.8));
            padding: 0.75rem;
            border-radius: 0.25rem;
            border: 1px solid rgba(0, 0, 0, 0.1);
        }

        /* 暗色主题下的详细信息项 */
        [data-bs-theme="dark"] .detail-item {
            background-color: rgba(52, 58, 64, var(--page-opacity, 0.8));
            border: 1px solid rgba(255, 255, 255, 0.1);
            color: #e0e0e0;
        }
        .detail-item strong {
            display: block;
            margin-bottom: 0.25rem;
        }
        .history-bar-container {
            display: inline-flex; /* Changed to inline-flex for centering within td */
            flex-direction: row-reverse; /* Newest on the right */
            align-items: center;
            justify-content: center; /* Center the bars within this container */
            height: 25px; /* Increased height */
            gap: 2px; /* Space between bars */
        }
        .history-bar {
            width: 8px; /* Increased width of each bar */
            height: 100%;
            /* margin-left: 1px; /* Replaced by gap */
            border-radius: 1px;
        }
        .history-bar-up { background-color: #28a745; } /* Green */
        .history-bar-down { background-color: #dc3545; } /* Red */
        .history-bar-pending { background-color: #6c757d; } /* Gray */
        .history-bar-high-latency { background-color: #f6b26b; } /* Light orange - 高延迟 */
        .provider-group-row > td {
            background-color: #eef4ff !important;
            border-top: 1px solid #d7e3ff;
            border-bottom: 1px solid #d7e3ff;
            padding-top: 0.75rem;
            padding-bottom: 0.75rem;
        }
        .provider-group-row .btn-link {
            font-weight: 600;
            color: inherit;
        }
        .provider-group-row .btn-link:hover {
            color: inherit;
        }
        .provider-child-row > td:first-child {
            padding-left: 1.75rem;
        }

        /* Default styling for progress bar text (light mode) */
        .progress span {
            color: #000000; /* Black text for progress bars by default */
            /* font-weight: bold; is handled by inline style in JS */
        }

        /* Center alignment for front-end monitoring tables */
        /* Front-end server monitoring table headers and data */
        .table > thead > tr > th:nth-child(1), /* 名称 */
        .table > thead > tr > th:nth-child(2), /* 状态 */
        .table > thead > tr > th:nth-child(3), /* CPU */
        .table > thead > tr > th:nth-child(4), /* 内存 */
        .table > thead > tr > th:nth-child(5), /* 硬盘 */
        .table > thead > tr > th:nth-child(6), /* 上传 */
        .table > thead > tr > th:nth-child(7), /* 下载 */
        .table > thead > tr > th:nth-child(8), /* 总上传 */
        .table > thead > tr > th:nth-child(9), /* 总下载 */
        .table > thead > tr > th:nth-child(10), /* 运行时长 */
        .table > thead > tr > th:nth-child(11), /* 最后更新 */
        #serverTableBody tr > td:nth-child(1), /* 名称 */
        #serverTableBody tr > td:nth-child(2), /* 状态 */
        #serverTableBody tr > td:nth-child(3), /* CPU */
        #serverTableBody tr > td:nth-child(4), /* 内存 */
        #serverTableBody tr > td:nth-child(5), /* 硬盘 */
        #serverTableBody tr > td:nth-child(6), /* 上传 */
        #serverTableBody tr > td:nth-child(7), /* 下载 */
        #serverTableBody tr > td:nth-child(8), /* 总上传 */
        #serverTableBody tr > td:nth-child(9), /* 总下载 */
        #serverTableBody tr > td:nth-child(10), /* 运行时长 */
        #serverTableBody tr > td:nth-child(11) { /* 最后更新 */
            text-align: center;
        }

        /* Front-end site monitoring table headers and data */
        .table > thead > tr > th:nth-child(1), /* 名称 (site table) */
        .table > thead > tr > th:nth-child(2), /* 状态 (site table) */
        .table > thead > tr > th:nth-child(3), /* 状态码 (site table) */
        .table > thead > tr > th:nth-child(4), /* 响应时间 (site table) */
        .table > thead > tr > th:nth-child(5), /* 最后检查 (site table) */
        .table > thead > tr > th:nth-child(6), /* 24h记录 (site table) */
        #siteStatusTableBody tr > td:nth-child(1) { /* 名称 - 左对齐 */
            text-align: left;
        }
        #siteStatusTableBody tr > td:nth-child(2), /* 状态 */
        #siteStatusTableBody tr > td:nth-child(3), /* 状态码 */
        #siteStatusTableBody tr > td:nth-child(4), /* 响应时间 */
        #siteStatusTableBody tr > td:nth-child(5), /* 最后检查 */
        #siteStatusTableBody tr > td:nth-child(6) { /* 24h记录 */
            text-align: center;
        }

        /* Backend admin tables - center align headers and data columns */
        /* Admin server table headers */
        .table thead tr th:nth-child(2), /* ID */
        .table thead tr th:nth-child(3), /* 名称 */
        .table thead tr th:nth-child(4), /* 描述 */
        .table thead tr th:nth-child(5), /* 状态 */
        .table thead tr th:nth-child(6), /* 最后更新 */
        .table thead tr th:nth-child(9), /* 显示开关 */
        /* Admin server table data */
        #serverTableBody tr > td:nth-child(2), /* ID */
        #serverTableBody tr > td:nth-child(3), /* 名称 */
        #serverTableBody tr > td:nth-child(4), /* 描述 */
        #serverTableBody tr > td:nth-child(5), /* 状态 */
        #serverTableBody tr > td:nth-child(6), /* 最后更新 */
        #serverTableBody tr > td:nth-child(9) { /* 显示开关 */
            text-align: center;
        }

        /* Admin site table headers */
        .table thead tr th:nth-child(2), /* 名称 */
        .table thead tr th:nth-child(4), /* 状态 */
        .table thead tr th:nth-child(5), /* 状态码 */
        .table thead tr th:nth-child(6), /* 响应时间 */
        .table thead tr th:nth-child(7), /* 最后检查 */
        .table thead tr th:nth-child(8), /* 显示开关 */
        /* Admin site table data */
        #siteTableBody tr > td:nth-child(2), /* 名称 */
        #siteTableBody tr > td:nth-child(4), /* 状态 */
        #siteTableBody tr > td:nth-child(5), /* 状态码 */
        #siteTableBody tr > td:nth-child(6), /* 响应时间 */
        #siteTableBody tr > td:nth-child(7), /* 最后检查 */
        #siteTableBody tr > td:nth-child(8) { /* 显示开关 */
            text-align: center;
        }

        /* Dark Theme Adjustments */
        [data-bs-theme="dark"] body {
            background-color: #212529 !important; /* Bootstrap dark bg */
            color: #ffffff !important; /* White text for dark mode */
        }
        [data-bs-theme="dark"] h1, [data-bs-theme="dark"] h2, [data-bs-theme="dark"] h3, [data-bs-theme="dark"] h4, [data-bs-theme="dark"] h5, [data-bs-theme="dark"] h6 {
            color: #ffffff; /* White color for headings */
        }
        [data-bs-theme="dark"] a:not(.btn):not(.nav-link):not(.dropdown-item):not(.navbar-brand) {
            color: #87cefa; /* LightSkyBlue for general links, good contrast on dark */
        }
        [data-bs-theme="dark"] a:not(.btn):not(.nav-link):not(.dropdown-item):not(.navbar-brand):hover {
            color: #add8e6; /* Lighter blue on hover */
        }
        [data-bs-theme="dark"] .navbar-dark {
            background-color: #343a40 !important; /* Darker navbar */
        }
        [data-bs-theme="dark"] .table {
            color: #ffffff; /* White table text */
        }
        [data-bs-theme="dark"] .table-striped > tbody > tr:nth-of-type(odd) > * {
            --bs-table-accent-bg: rgba(255, 255, 255, 0.05); /* Darker stripe */
            color: #ffffff; /* Ensure text in striped rows is white */
        }
        [data-bs-theme="dark"] .table-hover > tbody > tr:hover > * {
            --bs-table-accent-bg: rgba(255, 255, 255, 0.075); /* Darker hover */
            color: #ffffff; /* Ensure text in hovered rows is white */
        }
        [data-bs-theme="dark"] .server-details-row td {
            background-color: rgba(33, 37, 41, var(--page-opacity, 0.8)); /* Darker details background with transparency */
            border-top: 1px solid #495057;
        }
        [data-bs-theme="dark"] .detail-item {
            background-color: rgba(52, 58, 64, var(--page-opacity, 0.8)); /* Darker detail item background with transparency */
            border: 1px solid rgba(255, 255, 255, 0.1);
            color: #e0e0e0; /* Consistent text color */
        }
        [data-bs-theme="dark"] .progress {
            background-color: #495057; /* Darker progress bar background */
        }
        [data-bs-theme="dark"] .progress span { /* Text on progress bar */
            color: #000000 !important; /* Black text for progress bars */
            text-shadow: none; /* Remove shadow for black text or use a very light one if needed */
        }
        [data-bs-theme="dark"] .footer.bg-light {
            background-color: #343a40 !important; /* Darker footer */
            border-top: 1px solid #495057;
        }
        /* 已移至统一的底部版权样式中 */
        [data-bs-theme="dark"] .alert-info {
            background-color: #17a2b8; /* Bootstrap info color, adjust if needed */
            color: #fff;
            border-color: #17a2b8;
        }
        [data-bs-theme="dark"] .btn-outline-light {
            color: #f8f9fa;
            border-color: #f8f9fa;
        }
        [data-bs-theme="dark"] .btn-outline-light:hover {
            color: #212529;
            background-color: #f8f9fa;
        }
        [data-bs-theme="dark"] .card {
            background-color: #343a40;
            border: 1px solid #495057;
        }
        [data-bs-theme="dark"] .card-header {
            background-color: #495057;
            border-bottom: 1px solid #5b6167;
        }
        [data-bs-theme="dark"] .modal-content {
            background-color: #343a40;
            color: #ffffff; /* White modal text */
        }
        [data-bs-theme="dark"] .modal-header {
            border-bottom-color: #495057;
        }
        [data-bs-theme="dark"] .modal-footer {
            border-top-color: #495057;
        }
        [data-bs-theme="dark"] .form-control {
            background-color: #495057;
            color: #ffffff; /* White form control text */
            border-color: #5b6167;
        }
        [data-bs-theme="dark"] .form-control:focus {
            background-color: #495057;
            color: #ffffff; /* White form control text on focus */
            border-color: #86b7fe; /* Bootstrap focus color */
            box-shadow: 0 0 0 0.25rem rgba(13, 110, 253, 0.25);
        }
        [data-bs-theme="dark"] .form-label {
            color: #adb5bd;
        }
        [data-bs-theme="dark"] .text-danger { /* Ensure custom text-danger is visible */
            color: #ff8888 !important;
        }
        /* 通用text-muted主题适配 */
        .text-muted { color: #212529 !important; }
        [data-bs-theme="dark"] .text-muted { color: #ffffff !important; }
        [data-bs-theme="dark"] span[style*="color: #000"] { /* For inline styled black text */
            color: #ffffff !important; /* Change to white */
        }

        /* 拖拽排序样式 */
        .server-row-draggable, .site-row-draggable, .llm-row-draggable {
            transition: all 0.2s ease;
        }
        .server-row-draggable:hover, .site-row-draggable:hover, .llm-row-draggable:hover {
            background-color: rgba(0, 123, 255, 0.1) !important;
        }
        .server-row-draggable.drag-over-top, .site-row-draggable.drag-over-top, .llm-row-draggable.drag-over-top {
            border-top: 3px solid #007bff !important;
            background-color: rgba(0, 123, 255, 0.1) !important;
        }
        .server-row-draggable.drag-over-bottom, .site-row-draggable.drag-over-bottom, .llm-row-draggable.drag-over-bottom {
            border-bottom: 3px solid #007bff !important;
            background-color: rgba(0, 123, 255, 0.1) !important;
        }
        .server-row-draggable[draggable="true"], .site-row-draggable[draggable="true"], .llm-row-draggable[draggable="true"] {
            cursor: grab;
        }
        .server-row-draggable[draggable="true"]:active, .site-row-draggable[draggable="true"]:active, .llm-row-draggable[draggable="true"]:active {
            cursor: grabbing;
        }

        /* 暗色主题下的拖拽样式 */
        [data-bs-theme="dark"] .server-row-draggable:hover,
        [data-bs-theme="dark"] .site-row-draggable:hover,
        [data-bs-theme="dark"] .llm-row-draggable:hover {
            background-color: rgba(13, 110, 253, 0.2) !important;
        }
        [data-bs-theme="dark"] .server-row-draggable.drag-over-top,
        [data-bs-theme="dark"] .site-row-draggable.drag-over-top,
        [data-bs-theme="dark"] .llm-row-draggable.drag-over-top {
            border-top: 3px solid #0d6efd !important;
            background-color: rgba(13, 110, 253, 0.2) !important;
        }
        [data-bs-theme="dark"] .server-row-draggable.drag-over-bottom,
        [data-bs-theme="dark"] .site-row-draggable.drag-over-bottom,
        [data-bs-theme="dark"] .llm-row-draggable.drag-over-bottom {
            border-bottom: 3px solid #0d6efd !important;
            background-color: rgba(13, 110, 253, 0.2) !important;
        }
    </style>
</head>
<body>
    <!-- Toast容器 -->
    <div id="toastContainer" class="toast-container"></div>

    <nav class="navbar navbar-dark bg-primary">
        <div class="container">
            <a class="navbar-brand" href="/">
                <svg class="me-2" width="32" height="32" viewBox="0 0 32 32">
                    <defs>
                        <radialGradient id="navBg1" cx="0.3" cy="0.3">
                            <stop offset="0%" stop-color="#fff" stop-opacity="0.9"/>
                            <stop offset="100%" stop-color="#0277bd" stop-opacity="0.8"/>
                        </radialGradient>
                        <linearGradient id="navEcg1" x1="0%" x2="100%">
                            <stop offset="0%" stop-color="#f08"/>
                            <stop offset="50%" stop-color="#0f8"/>
                            <stop offset="100%" stop-color="#80f"/>
                        </linearGradient>
                    </defs>
                    <circle cx="16" cy="16" r="15" fill="url(#navBg1)" stroke="#0277bd" stroke-width="1.5"/>
                    <circle cx="16" cy="16" r="13" fill="none" stroke="#fff" stroke-width="1" opacity="0.4"/>
                    <line x1="4" y1="16" x2="28" y2="16" stroke="#b3e5fc" stroke-width="0.5" opacity="0.8"/>
                    <path id="navP1" d="M4 16L8 16L9 15L10 17L11 14L12 18L13 10L14 22L15 16L28 16" fill="none" stroke="url(#navEcg1)" stroke-width="2.8"/>
                    <path d="M4 16L8 16L9 15L10 17L11 14L12 18L13 10L14 22L15 16L28 16" fill="none" stroke="#fff" stroke-width="1.2" opacity="0.7"/>
                    <circle r="1.5" fill="#fff">
                        <animateMotion dur="2s" repeatCount="indefinite">
                            <mpath href="#navP1"/>
                        </animateMotion>
                    </circle>
                    <circle cx="16" cy="16" r="8" fill="none" stroke="#f08" stroke-width="0.5" opacity="0.6">
                        <animate attributeName="r" values="8;12;8" dur="3s" repeatCount="indefinite"/>
                        <animate attributeName="opacity" values="0.6;0;0.6" dur="3s" repeatCount="indefinite"/>
                    </circle>
                </svg>
                VPS监控面板
            </a>
            <div class="d-flex align-items-center">
                <a href="https://github.com/kadidalax/cf-vps-monitor" target="_blank" rel="noopener noreferrer" class="btn btn-outline-light btn-sm me-2" title="GitHub Repository">
                    <i class="bi bi-github"></i>
                </a>
                <button id="themeToggler" class="btn btn-outline-light btn-sm me-2" title="切换主题">
                    <i class="bi bi-moon-stars-fill"></i>
                </button>
                <a class="nav-link text-light" id="adminAuthLink" href="/login.html" style="white-space: nowrap;">管理员登录</a>
            </div>
        </div>
    </nav>

    <!-- 单一主卡片容器 -->
    <div class="container mt-4">
        <div class="card shadow-sm">
            <div class="card-body">
                <!-- 服务器监控部分 -->
                <div class="mb-4">
                    <h5 class="card-title mb-3">
                        <i class="bi bi-server me-2"></i>服务器监控
                    </h5>

                    <div id="noServers" class="alert alert-info d-none">
                        暂无服务器数据，请先登录管理后台添加服务器。
                    </div>

                    <!-- 桌面端表格视图 -->
                    <div class="table-responsive">
                        <table class="table table-striped table-hover align-middle">
                            <thead>
                                <tr>
                                    <th>名称</th>
                                    <th>状态</th>
                                    <th>CPU</th>
                                    <th>内存</th>
                                    <th>硬盘</th>
                                    <th>上传</th>
                                    <th>下载</th>
                                    <th>总上传</th>
                                    <th>总下载</th>
                                    <th>运行时长</th>
                                    <th>最后更新</th>
                                </tr>
                            </thead>
                            <tbody id="serverTableBody">
                                <tr>
                                    <td colspan="11" class="text-center">加载中...</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>

                    <!-- 移动端卡片视图 -->
                    <div class="mobile-card-container" id="mobileServerContainer">
                        <div class="text-center p-3">
                            <div class="spinner-border text-primary" role="status">
                                <span class="visually-hidden">加载中...</span>
                            </div>
                            <div class="mt-2">加载服务器数据中...</div>
                        </div>
                    </div>
                </div>

                <!-- 分隔线 -->
                <hr class="my-4">

                <!-- 网站监控部分 -->
                <div>
                    <h5 class="card-title mb-3">
                        <i class="bi bi-globe me-2"></i>网站在线状态
                    </h5>

                    <div id="noSites" class="alert alert-info d-none">
                        暂无监控网站数据。
                    </div>

                    <!-- 桌面端表格视图 -->
                    <div class="table-responsive">
                        <table class="table table-striped table-hover align-middle">
                            <thead>
                                <tr>
                                    <th>名称</th>
                                    <th>状态</th>
                                    <th>状态码</th>
                                    <th>响应时间 (ms)</th>
                                    <th>最后检查</th>
                                    <th>24h记录</th>
                                </tr>
                            </thead>
                            <tbody id="siteStatusTableBody">
                                <tr>
                                    <td colspan="6" class="text-center">加载中...</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>

                    <!-- 移动端卡片视图 -->
                    <div class="mobile-card-container" id="mobileSiteContainer">
                        <div class="text-center p-3">
                            <div class="spinner-border text-primary" role="status">
                                <span class="visually-hidden">加载中...</span>
                            </div>
                            <div class="mt-2">加载网站数据中...</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    <!-- End Website Status Section -->

    <!-- LLM Endpoint Status Section -->
    <div class="container mt-4 mb-4">
        <div class="card shadow-sm">
            <div class="card-body">
                <div>
                    <h5 class="card-title mb-3">
                        <i class="bi bi-robot me-2"></i>LLM 端点可用性
                    </h5>

                    <div id="noLlmEndpoints" class="alert alert-info d-none">
                        暂无监控 LLM 端点。
                    </div>

                    <!-- 桌面端表格视图 -->
                    <div class="table-responsive">
                        <table class="table table-striped table-hover align-middle">
                            <thead>
                                <tr>
                                    <th>Provider / 模型</th>
                                    <th>状态</th>
                                    <th>状态码</th>
                                    <th>响应时间 (ms)</th>
                                    <th>最后检查</th>
                                    <th>输出预览</th>
                                    <th>24h记录</th>
                                </tr>
                            </thead>
                            <tbody id="llmStatusTableBody">
                                <tr>
                                    <td colspan="7" class="text-center">加载中...</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>

                    <!-- 移动端卡片视图 -->
                    <div class="mobile-card-container" id="mobileLlmContainer">
                        <div class="text-center p-3">
                            <div class="spinner-border text-primary" role="status">
                                <span class="visually-hidden">加载中...</span>
                            </div>
                            <div class="mt-2">加载LLM端点数据中...</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    <!-- End LLM Endpoint Status Section -->

    <!-- Server Detailed row template (hidden by default) -->
    <template id="serverDetailsTemplate">
        <tr class="server-details-row d-none">
            <td colspan="11">
                <div class="server-details-content">
                    <!-- Detailed metrics will be populated here by JavaScript -->
                </div>
            </td>
        </tr>
    </template>

    <footer class="footer fixed-bottom py-2 bg-light border-top">
        <div class="container text-center">
            <span class="text-muted small">VPS监控面板 &copy; 2025</span>
            <a href="https://github.com/kadidalax/cf-vps-monitor" target="_blank" rel="noopener noreferrer" class="ms-3 text-muted" title="GitHub Repository">
                <i class="bi bi-github"></i>
            </a>
        </div>
    </footer>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js" integrity="sha384-C6RzsynM9kWDrMNeT87bh95OGNyZPhcTNXj1NW7RuBCsyN/o0jlpcV8Qyq46cDfL" crossorigin="anonymous"></script>
    <script src="/js/main.js"></script>
</body>
</html>`;
}

function getLoginHtml() {
  return `<!DOCTYPE html>
<html lang="zh-CN" data-bs-theme="light">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>登录 - VPS监控面板</title>
    <link rel="icon" type="image/svg+xml" href="/favicon.svg">
    <script>
        // 立即设置主题，避免闪烁
        (function() {
            const theme = localStorage.getItem('vps-monitor-theme') || 'light';
            document.documentElement.setAttribute('data-bs-theme', theme);
        })();
    </script>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-T3c6CoIi6uLrA9TneNEoa7RxnatzjcDSCmG1MXxSR1GAsXEV/Dwwykc2MPK8M2HN" crossorigin="anonymous">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/font/bootstrap-icons.css" rel="stylesheet" integrity="sha384-4LISF5TTJX/fLmGSxO53rV4miRxdg84mZsxmO8Rx5jGtp/LbrixFETvWa5a6sESd" crossorigin="anonymous">
    <link href="/css/style.css" rel="stylesheet">
    <style>
        .server-row {
            cursor: pointer; /* Indicate clickable rows */
        }
        .server-details-row {
            /* display: none; /* Initially hidden - controlled by JS */ */
        }
        .server-details-row td {
            padding: 1rem;
            background-color: rgba(248, 249, 250, var(--page-opacity, 0.8)); /* Light background for details with transparency */
        }

        /* 暗色主题下的服务器详细信息行 */
        [data-bs-theme="dark"] .server-details-row td {
            background-color: rgba(33, 37, 41, var(--page-opacity, 0.8));
        }
        .server-details-content {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
        }
        .detail-item {
            background-color: rgba(233, 236, 239, var(--page-opacity, 0.8));
            padding: 0.75rem;
            border-radius: 0.25rem;
            border: 1px solid rgba(0, 0, 0, 0.1);
        }

        /* 暗色主题下的详细信息项 */
        [data-bs-theme="dark"] .detail-item {
            background-color: rgba(52, 58, 64, var(--page-opacity, 0.8));
            border: 1px solid rgba(255, 255, 255, 0.1);
            color: #e0e0e0;
        }
        .detail-item strong {
            display: block;
            margin-bottom: 0.25rem;
        }
        .history-bar-container {
            display: inline-flex; /* Changed to inline-flex for centering within td */
            flex-direction: row-reverse; /* Newest on the right */
            align-items: center;
            justify-content: center; /* Center the bars within this container */
            height: 25px; /* Increased height */
            gap: 2px; /* Space between bars */
        }
        .history-bar {
            width: 8px; /* Increased width of each bar */
            height: 100%;
            /* margin-left: 1px; /* Replaced by gap */
            border-radius: 1px;
        }
        .history-bar-up { background-color: #28a745; } /* Green */
        .history-bar-down { background-color: #dc3545; } /* Red */
        .history-bar-pending { background-color: #6c757d; } /* Gray */
        .history-bar-high-latency { background-color: #f6b26b; } /* Light orange - 高延迟 */
        .provider-group-row > td {
            background-color: #eef4ff !important;
            border-top: 1px solid #d7e3ff;
            border-bottom: 1px solid #d7e3ff;
            padding-top: 0.75rem;
            padding-bottom: 0.75rem;
        }
        .provider-group-row .btn-link {
            font-weight: 600;
            color: inherit;
        }
        .provider-group-row .btn-link:hover {
            color: inherit;
        }
        .provider-child-row > td:first-child {
            padding-left: 1.75rem;
        }

        /* Default styling for progress bar text (light mode) */
        .progress span {
            color: #000000; /* Black text for progress bars by default */
            /* font-weight: bold; is handled by inline style in JS */
        }

        /* Center the "24h记录" (site table) and "上传" (server table) headers and their data cells */
        .table > thead > tr > th:nth-child(6), /* Targets 6th header in both tables */
        #siteStatusTableBody tr > td:nth-child(6), /* Targets 6th data cell in site status table */
        #serverTableBody tr > td:nth-child(6) { /* Targets 6th data cell in server status table */
            text-align: center;
        }

        /* Dark Theme Adjustments */
        [data-bs-theme="dark"] body {
            background-color: #212529; /* Bootstrap dark bg */
            color: #ffffff; /* White text for dark mode */
        }
        [data-bs-theme="dark"] h1, [data-bs-theme="dark"] h2, [data-bs-theme="dark"] h3, [data-bs-theme="dark"] h4, [data-bs-theme="dark"] h5, [data-bs-theme="dark"] h6 {
            color: #ffffff; /* White color for headings */
        }
        [data-bs-theme="dark"] a:not(.btn):not(.nav-link):not(.dropdown-item):not(.navbar-brand) {
            color: #87cefa; /* LightSkyBlue for general links, good contrast on dark */
        }
        [data-bs-theme="dark"] a:not(.btn):not(.nav-link):not(.dropdown-item):not(.navbar-brand):hover {
            color: #add8e6; /* Lighter blue on hover */
        }
        [data-bs-theme="dark"] .navbar-dark {
            background-color: #343a40 !important; /* Darker navbar */
        }
        [data-bs-theme="dark"] .table {
            color: #ffffff; /* White table text */
        }
        [data-bs-theme="dark"] .table-striped > tbody > tr:nth-of-type(odd) > * {
            --bs-table-accent-bg: rgba(255, 255, 255, 0.05); /* Darker stripe */
            color: #ffffff; /* Ensure text in striped rows is white */
        }
        [data-bs-theme="dark"] .table-hover > tbody > tr:hover > * {
            --bs-table-accent-bg: rgba(255, 255, 255, 0.075); /* Darker hover */
            color: #ffffff; /* Ensure text in hovered rows is white */
        }
        [data-bs-theme="dark"] .server-details-row td {
            background-color: rgba(33, 37, 41, var(--page-opacity, 0.8)); /* Darker details background with transparency */
            border-top: 1px solid #495057;
        }
        [data-bs-theme="dark"] .detail-item {
            background-color: rgba(52, 58, 64, var(--page-opacity, 0.8)); /* Darker detail item background with transparency */
            border: 1px solid rgba(255, 255, 255, 0.1);
            color: #e0e0e0; /* Consistent text color */
        }
        [data-bs-theme="dark"] .progress {
            background-color: #495057; /* Darker progress bar background */
        }
        [data-bs-theme="dark"] .progress span { /* Text on progress bar */
            color: #000000 !important; /* Black text for progress bars */
            text-shadow: none; /* Remove shadow for black text or use a very light one if needed */
        }
        [data-bs-theme="dark"] .footer.bg-light {
            background-color: #343a40 !important; /* Darker footer */
            border-top: 1px solid #495057;
        }
        /* 已移至统一的底部版权样式中 */
        [data-bs-theme="dark"] .alert-info {
            background-color: #17a2b8; /* Bootstrap info color, adjust if needed */
            color: #fff;
            border-color: #17a2b8;
        }
        [data-bs-theme="dark"] .btn-outline-light {
            color: #f8f9fa;
            border-color: #f8f9fa;
        }
        [data-bs-theme="dark"] .btn-outline-light:hover {
            color: #212529;
            background-color: #f8f9fa;
        }
        [data-bs-theme="dark"] .card {
            background-color: #343a40;
            border: 1px solid #495057;
        }
        [data-bs-theme="dark"] .card-header {
            background-color: #495057;
            border-bottom: 1px solid #5b6167;
        }
        [data-bs-theme="dark"] .modal-content {
            background-color: #343a40;
            color: #ffffff; /* White modal text */
        }
        [data-bs-theme="dark"] .modal-header {
            border-bottom-color: #495057;
        }
        [data-bs-theme="dark"] .modal-footer {
            border-top-color: #495057;
        }
        [data-bs-theme="dark"] .form-control {
            background-color: #495057;
            color: #ffffff; /* White form control text */
            border-color: #5b6167;
        }
        [data-bs-theme="dark"] .form-control:focus {
            background-color: #495057;
            color: #ffffff; /* White form control text on focus */
            border-color: #86b7fe; /* Bootstrap focus color */
            box-shadow: 0 0 0 0.25rem rgba(13, 110, 253, 0.25);
        }
        [data-bs-theme="dark"] .form-label {
            color: #adb5bd;
        }
        [data-bs-theme="dark"] .text-danger { /* Ensure custom text-danger is visible */
            color: #ff8888 !important;
        }
        /* 已移至统一的通用text-muted样式中 */
        [data-bs-theme="dark"] span[style*="color: #000"] { /* For inline styled black text */
            color: #ffffff !important; /* Change to white */
        }
    </style>
</head>
<body>
    <!-- Toast容器 -->
    <div id="toastContainer" class="toast-container"></div>

    <nav class="navbar navbar-dark bg-primary">
        <div class="container">
            <a class="navbar-brand" href="/">
                <svg class="me-2" width="32" height="32" viewBox="0 0 32 32">
                    <defs>
                        <radialGradient id="navBg2" cx="0.3" cy="0.3">
                            <stop offset="0%" stop-color="#fff" stop-opacity="0.9"/>
                            <stop offset="100%" stop-color="#0277bd" stop-opacity="0.8"/>
                        </radialGradient>
                        <linearGradient id="navEcg2" x1="0%" x2="100%">
                            <stop offset="0%" stop-color="#f08"/>
                            <stop offset="50%" stop-color="#0f8"/>
                            <stop offset="100%" stop-color="#80f"/>
                        </linearGradient>
                    </defs>
                    <circle cx="16" cy="16" r="15" fill="url(#navBg2)" stroke="#0277bd" stroke-width="1.5"/>
                    <circle cx="16" cy="16" r="13" fill="none" stroke="#fff" stroke-width="1" opacity="0.4"/>
                    <line x1="4" y1="16" x2="28" y2="16" stroke="#b3e5fc" stroke-width="0.5" opacity="0.8"/>
                    <path id="navP2" d="M4 16L8 16L9 15L10 17L11 14L12 18L13 10L14 22L15 16L28 16" fill="none" stroke="url(#navEcg2)" stroke-width="2.8"/>
                    <path d="M4 16L8 16L9 15L10 17L11 14L12 18L13 10L14 22L15 16L28 16" fill="none" stroke="#fff" stroke-width="1.2" opacity="0.7"/>
                    <circle r="1.5" fill="#fff">
                        <animateMotion dur="2s" repeatCount="indefinite">
                            <mpath href="#navP2"/>
                        </animateMotion>
                    </circle>
                    <circle cx="16" cy="16" r="8" fill="none" stroke="#f08" stroke-width="0.5" opacity="0.6">
                        <animate attributeName="r" values="8;12;8" dur="3s" repeatCount="indefinite"/>
                        <animate attributeName="opacity" values="0.6;0;0.6" dur="3s" repeatCount="indefinite"/>
                    </circle>
                </svg>
                VPS监控面板
            </a>
            <div class="d-flex align-items-center">
                <button id="themeToggler" class="btn btn-outline-light btn-sm me-2" title="切换主题">
                    <i class="bi bi-moon-stars-fill"></i>
                </button>
                <a class="nav-link text-light" href="/" style="white-space: nowrap;">返回首页</a>
            </div>
        </div>
    </nav>

    <div class="container mt-5">
        <div class="row justify-content-center">
            <div class="col-md-6 col-lg-4">
                <div class="card">
                    <div class="card-header">
                        <h4 class="card-title mb-0">管理员登录</h4>
                    </div>
                    <div class="card-body">

                        <form id="loginForm">
                            <div class="mb-3">
                                <label for="username" class="form-label">用户名</label>
                                <input type="text" class="form-control" id="username" required>
                            </div>
                            <div class="mb-3">
                                <label for="password" class="form-label">密码</label>
                                <input type="password" class="form-control" id="password" required>
                            </div>
                            <div class="d-grid">
                                <button type="submit" class="btn btn-primary">登录</button>
                            </div>
                        </form>
                    </div>
                    <div class="card-footer text-muted">
                        <small id="defaultCredentialsInfo">加载默认凭据信息中...</small>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <footer class="footer fixed-bottom py-2 bg-light border-top">
        <div class="container text-center">
            <span class="text-muted small">VPS监控面板 &copy; 2025</span>
            <a href="https://github.com/kadidalax/cf-vps-monitor" target="_blank" rel="noopener noreferrer" class="ms-3 text-muted" title="GitHub Repository">
                <i class="bi bi-github"></i>
            </a>
        </div>
    </footer>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js" integrity="sha384-C6RzsynM9kWDrMNeT87bh95OGNyZPhcTNXj1NW7RuBCsyN/o0jlpcV8Qyq46cDfL" crossorigin="anonymous"></script>
    <script src="/js/login.js"></script>
</body>
</html>`;
}

function getAdminHtml() {
  return `<!DOCTYPE html>
<html lang="zh-CN" data-bs-theme="light">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>管理后台 - VPS监控面板</title>
    <link rel="icon" type="image/svg+xml" href="/favicon.svg">
    <script>
        // 立即设置主题，避免闪烁
        (function() {
            const theme = localStorage.getItem('vps-monitor-theme') || 'light';
            document.documentElement.setAttribute('data-bs-theme', theme);
        })();
    </script>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-T3c6CoIi6uLrA9TneNEoa7RxnatzjcDSCmG1MXxSR1GAsXEV/Dwwykc2MPK8M2HN" crossorigin="anonymous">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/font/bootstrap-icons.css" rel="stylesheet" integrity="sha384-4LISF5TTJX/fLmGSxO53rV4miRxdg84mZsxmO8Rx5jGtp/LbrixFETvWa5a6sESd" crossorigin="anonymous">
    <link href="/css/style.css" rel="stylesheet">
</head>
<body>
    <!-- Toast容器 -->
    <div id="toastContainer" class="toast-container"></div>

    <nav class="navbar navbar-dark bg-primary">
        <div class="container">
            <a class="navbar-brand" href="/">
                <svg class="me-2" width="32" height="32" viewBox="0 0 32 32">
                    <defs>
                        <radialGradient id="navBg3" cx="0.3" cy="0.3">
                            <stop offset="0%" stop-color="#fff" stop-opacity="0.9"/>
                            <stop offset="100%" stop-color="#0277bd" stop-opacity="0.8"/>
                        </radialGradient>
                        <linearGradient id="navEcg3" x1="0%" x2="100%">
                            <stop offset="0%" stop-color="#f08"/>
                            <stop offset="50%" stop-color="#0f8"/>
                            <stop offset="100%" stop-color="#80f"/>
                        </linearGradient>
                    </defs>
                    <circle cx="16" cy="16" r="15" fill="url(#navBg3)" stroke="#0277bd" stroke-width="1.5"/>
                    <circle cx="16" cy="16" r="13" fill="none" stroke="#fff" stroke-width="1" opacity="0.4"/>
                    <line x1="4" y1="16" x2="28" y2="16" stroke="#b3e5fc" stroke-width="0.5" opacity="0.8"/>
                    <path id="navP3" d="M4 16L8 16L9 15L10 17L11 14L12 18L13 10L14 22L15 16L28 16" fill="none" stroke="url(#navEcg3)" stroke-width="2.8"/>
                    <path d="M4 16L8 16L9 15L10 17L11 14L12 18L13 10L14 22L15 16L28 16" fill="none" stroke="#fff" stroke-width="1.2" opacity="0.7"/>
                    <circle r="1.5" fill="#fff">
                        <animateMotion dur="2s" repeatCount="indefinite">
                            <mpath href="#navP3"/>
                        </animateMotion>
                    </circle>
                    <circle cx="16" cy="16" r="8" fill="none" stroke="#f08" stroke-width="0.5" opacity="0.6">
                        <animate attributeName="r" values="8;12;8" dur="3s" repeatCount="indefinite"/>
                        <animate attributeName="opacity" values="0.6;0;0.6" dur="3s" repeatCount="indefinite"/>
                    </circle>
                </svg>
                VPS监控面板
            </a>
            <div class="d-flex align-items-center flex-wrap">
                <a class="nav-link text-light me-2" href="/" style="white-space: nowrap;">返回首页</a>

                <!-- PC端直接显示的按钮 -->
                <a href="https://github.com/kadidalax/cf-vps-monitor" target="_blank" rel="noopener noreferrer" class="btn btn-outline-light btn-sm me-2 desktop-only" title="GitHub Repository">
                    <i class="bi bi-github"></i>
                </a>

                <button id="themeToggler" class="btn btn-outline-light btn-sm me-2" title="切换主题">
                    <i class="bi bi-moon-stars-fill"></i>
                </button>

                <button class="btn btn-outline-light btn-sm me-1 desktop-only" id="changePasswordBtnDesktop" title="修改密码">
                    <i class="bi bi-key"></i>
                </button>

                <!-- 移动端下拉菜单 -->
                <div class="dropdown me-1 mobile-only">
                    <button class="btn btn-outline-light btn-sm dropdown-toggle" type="button" id="adminMenuDropdown" data-bs-toggle="dropdown" aria-expanded="false" title="更多选项">
                        <i class="bi bi-three-dots"></i>
                    </button>
                    <ul class="dropdown-menu dropdown-menu-end" aria-labelledby="adminMenuDropdown">
                        <li><a class="dropdown-item" href="https://github.com/kadidalax/cf-vps-monitor" target="_blank" rel="noopener noreferrer">
                            <i class="bi bi-github me-2"></i>GitHub
                        </a></li>
                        <li><button class="dropdown-item" id="changePasswordBtn">
                            <i class="bi bi-key me-2"></i>修改密码
                        </button></li>
                    </ul>
                </div>

                <button id="logoutBtn" class="btn btn-outline-light btn-sm" style="font-size: 0.75rem; padding: 0.25rem 0.5rem;">退出</button>
            </div>
        </div>
    </nav>

    <!-- 单一主管理卡片容器 -->
    <div class="container mt-4">
        <div class="card shadow-sm">
            <div class="card-body">
                <!-- 服务器管理部分 -->
                <div class="mb-4">
                    <div class="admin-header-row mb-3">
                        <div class="admin-header-title">
                            <h5 class="card-title mb-0">
                                <i class="bi bi-server me-2"></i>服务器管理
                            </h5>
                        </div>
                        <div class="admin-header-content">
                            <!-- VPS Data Update Frequency Form -->
                            <form id="globalSettingsFormPartial" class="admin-settings-form">
                                <div class="settings-group">
                                    <label for="vpsReportInterval" class="form-label">VPS数据更新频率 (秒):</label>
                                    <div class="input-group">
                                        <input type="number" class="form-control form-control-sm" id="vpsReportInterval" placeholder="例如: 60" min="1" style="width: 100px;">
                                        <button type="button" id="saveVpsReportIntervalBtn" class="btn btn-info btn-sm">保存频率</button>
                                    </div>
                                </div>
                            </form>

                            <!-- Action Buttons Group -->
                            <div class="admin-actions-group">
                                <!-- Server Auto Sort Dropdown -->
                                <div class="dropdown me-2">
                                    <button class="btn btn-outline-secondary dropdown-toggle" type="button" id="serverAutoSortDropdown" data-bs-toggle="dropdown" aria-expanded="false">
                                        <i class="bi bi-sort-alpha-down"></i> 自动排序
                                    </button>
                                    <ul class="dropdown-menu" aria-labelledby="serverAutoSortDropdown">
                                        <li><a class="dropdown-item active" href="#" onclick="autoSortServers('custom')">自定义排序</a></li>
                                        <li><a class="dropdown-item" href="#" onclick="autoSortServers('name')">按名称排序</a></li>
                                        <li><a class="dropdown-item" href="#" onclick="autoSortServers('status')">按状态排序</a></li>
                                    </ul>
                                </div>

                                <!-- Add Server Button -->
                                <button id="addServerBtn" class="btn btn-primary">
                                    <i class="bi bi-plus-circle"></i> 添加服务器
                                </button>
                            </div>
                        </div>
                    </div>



                    <!-- 桌面端表格视图 -->
                    <div class="table-responsive">
                        <table class="table table-striped table-hover">
                            <thead>
                                <tr>
                                    <th>排序</th>
                                    <th>ID</th>
                                    <th>名称</th>
                                    <th>描述</th>
                                    <th>状态</th>
                                    <th>最后更新</th>
                                    <th>API密钥</th>
                                    <th>VPS脚本</th>
                                    <th>显示 <i class="bi bi-question-circle text-muted" data-bs-toggle="tooltip" data-bs-placement="top" data-bs-title="是否对游客展示此服务器"></i></th>
                                    <th>操作</th>
                                </tr>
                            </thead>
                            <tbody id="serverTableBody">
                                <tr>
                                    <td colspan="10" class="text-center">加载中...</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>

                    <!-- 移动端卡片视图 -->
                    <div class="mobile-card-container" id="mobileAdminServerContainer">
                        <div class="text-center p-3">
                            <div class="spinner-border text-primary" role="status">
                                <span class="visually-hidden">加载中...</span>
                            </div>
                            <div class="mt-2">加载服务器数据中...</div>
                        </div>
                    </div>
                </div>

                <!-- 分隔线 -->
                <hr class="my-4">

                <!-- 网站监控管理部分 -->
                <div>
                    <div class="admin-header-row mb-3">
                        <div class="admin-header-title">
                            <h5 class="card-title mb-0">
                                <i class="bi bi-globe me-2"></i>网站监控管理
                            </h5>
                        </div>
                        <div class="admin-header-content">
                            <!-- Action Buttons Group - 桌面端隐藏，移动端显示居中按钮 -->
                            <div class="admin-actions-group desktop-only">
                                <!-- Site Auto Sort Dropdown -->
                                <div class="dropdown me-2">
                                    <button class="btn btn-outline-secondary dropdown-toggle" type="button" id="siteAutoSortDropdown" data-bs-toggle="dropdown" aria-expanded="false">
                                        <i class="bi bi-sort-alpha-down"></i> 自动排序
                                    </button>
                                    <ul class="dropdown-menu" aria-labelledby="siteAutoSortDropdown">
                                        <li><a class="dropdown-item active" href="#" onclick="autoSortSites('custom')">自定义排序</a></li>
                                        <li><a class="dropdown-item" href="#" onclick="autoSortSites('name')">按名称排序</a></li>
                                        <li><a class="dropdown-item" href="#" onclick="autoSortSites('url')">按URL排序</a></li>
                                        <li><a class="dropdown-item" href="#" onclick="autoSortSites('status')">按状态排序</a></li>
                                    </ul>
                                </div>

                                <button id="addSiteBtn" class="btn btn-success">
                                    <i class="bi bi-plus-circle"></i> 添加监控网站
                                </button>
                            </div>
                        </div>
                    </div>


                    <!-- 桌面端表格视图 -->
                    <div class="table-responsive">
                        <table class="table table-striped table-hover">
                            <thead>
                                <tr>
                                    <th>排序</th>
                                    <th>名称</th>
                                    <th>URL</th>
                                    <th>状态</th>
                                    <th>状态码</th>
                                    <th>响应时间 (ms)</th>
                                    <th>最后检查</th>
                                    <th>显示 <i class="bi bi-question-circle text-muted" data-bs-toggle="tooltip" data-bs-placement="top" data-bs-title="是否对游客展示此网站"></i></th>
                                    <th>操作</th>
                                </tr>
                            </thead>
                            <tbody id="siteTableBody">
                                <tr>
                                    <td colspan="9" class="text-center">加载中...</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>

                    <!-- 移动端卡片视图 -->
                    <div class="mobile-card-container" id="mobileAdminSiteContainer">
                        <div class="text-center p-3">
                            <div class="spinner-border text-primary" role="status">
                                <span class="visually-hidden">加载中...</span>
                            </div>
                            <div class="mt-2">加载网站数据中...</div>
                        </div>
                    </div>
                </div>

                <!-- 分隔线 -->
                <hr class="my-4">

                <!-- LLM端点监控管理部分 -->
                <div>
                    <div class="admin-header-row mb-3">
                        <div class="admin-header-title">
                            <h5 class="card-title mb-0">
                                <i class="bi bi-robot me-2"></i>LLM 端点监控管理
                            </h5>
                        </div>
                        <div class="admin-header-content">
                            <!-- LLM检查频率设置 -->
                            <form id="llmCheckIntervalForm" class="admin-settings-form me-2">
                                <div class="settings-group">
                                    <label for="llmCheckInterval" class="form-label">探测频率倍数:</label>
                                    <div class="input-group">
                                        <input type="number" class="form-control form-control-sm" id="llmCheckInterval" placeholder="1" min="1" style="width: 80px;">
                                        <button type="button" id="saveLlmCheckIntervalBtn" class="btn btn-info btn-sm">保存</button>
                                    </div>
                                    <small class="text-muted">1=每次Cron都检查，2=隔一次，3=每三次...</small>
                                </div>
                            </form>
                            <form id="highLatencyThresholdForm" class="admin-settings-form me-2">
                                <div class="settings-group">
                                    <label class="form-label mb-0">高延迟阈值:</label>
                                    <div class="d-flex flex-wrap gap-2 align-items-center">
                                        <div class="input-group input-group-sm" style="width: 170px;">
                                            <span class="input-group-text">网站</span>
                                            <input type="number" class="form-control" id="websiteHighLatencyMs" min="1" max="14999" placeholder="3000">
                                            <span class="input-group-text">ms</span>
                                        </div>
                                        <div class="input-group input-group-sm" style="width: 170px;">
                                            <span class="input-group-text">LLM</span>
                                            <input type="number" class="form-control" id="llmHighLatencyMs" min="1" max="119000" placeholder="20000">
                                            <span class="input-group-text">ms</span>
                                        </div>
                                        <button type="button" id="saveHighLatencyThresholdsBtn" class="btn btn-outline-warning btn-sm">保存阈值</button>
                                    </div>
                                </div>
                            </form>
                            <div class="admin-actions-group desktop-only">
                                <button id="addLlmEndpointBtn" class="btn btn-success">
                                    <i class="bi bi-plus-circle"></i> 添加LLM端点
                                </button>
                            </div>
                        </div>
                    </div>

                    <!-- 桌面端表格视图 -->
                    <div class="table-responsive">
                        <table class="table table-striped table-hover">
                            <thead>
                                <tr>
                                    <th style="width:30px"></th>
                                    <th>Provider / 模型</th>
                                    <th>API URL</th>
                                    <th>状态</th>
                                    <th>状态码</th>
                                    <th>响应时间 (ms)</th>
                                    <th>最后检查</th>
                                    <th>显示</th>
                                    <th>通知</th>
                                    <th>操作</th>
                                </tr>
                            </thead>
                            <tbody id="llmEndpointTableBody">
                                <tr>
                                    <td colspan="10" class="text-center">加载中...</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>

                    <!-- 移动端卡片视图 -->
                    <div class="mobile-card-container" id="mobileAdminLlmContainer">
                        <div class="text-center p-3">
                            <div class="spinner-border text-primary" role="status">
                                <span class="visually-hidden">加载中...</span>
                            </div>
                            <div class="mt-2">加载LLM端点数据中...</div>
                        </div>
                    </div>
                </div>

                <!-- 分隔线 -->
                <hr class="my-4">

                <!-- Telegram 通知设置部分 -->
                <div>
                    <h5 class="card-title mb-3">
                        <i class="bi bi-telegram me-2"></i>Telegram 通知设置
                    </h5>



                    <form id="telegramSettingsForm">
                        <div class="mb-3">
                            <label for="telegramBotToken" class="form-label">Bot Token</label>
                            <input type="text" class="form-control" id="telegramBotToken" placeholder="请输入 Telegram Bot Token">
                        </div>
                        <div class="mb-3">
                            <label for="telegramChatId" class="form-label">Chat ID</label>
                            <input type="text" class="form-control" id="telegramChatId" placeholder="请输入接收通知的 Chat ID">
                        </div>
                        <div class="form-check mb-3">
                            <input class="form-check-input" type="checkbox" id="enableTelegramNotifications">
                            <label class="form-check-label" for="enableTelegramNotifications">
                                启用通知
                            </label>
                        </div>
                        <button type="button" id="saveTelegramSettingsBtn" class="btn btn-info">保存Telegram设置</button>
                    </form>
                </div>

                <!-- 分隔线 -->
                <hr class="my-4">

                <!-- ntfy 通知设置部分 -->
                <div>
                    <h5 class="card-title mb-3">
                        <i class="bi bi-bell me-2"></i>ntfy 通知设置
                    </h5>

                    <form id="ntfySettingsForm">
                        <div class="mb-3">
                            <label for="ntfyTopic" class="form-label">Topic</label>
                            <input type="text" class="form-control" id="ntfyTopic" placeholder="请输入 ntfy topic 名称">
                        </div>
                        <div class="mb-3">
                            <label for="ntfyServerUrl" class="form-label">服务器地址</label>
                            <input type="text" class="form-control" id="ntfyServerUrl" placeholder="https://ntfy.sh" value="https://ntfy.sh">
                            <div class="form-text">默认为 https://ntfy.sh，自建服务器请修改为你的地址</div>
                        </div>
                        <div class="form-check mb-3">
                            <input class="form-check-input" type="checkbox" id="enableNtfyNotifications">
                            <label class="form-check-label" for="enableNtfyNotifications">
                                启用通知
                            </label>
                        </div>
                        <button type="button" id="saveNtfySettingsBtn" class="btn btn-info">保存ntfy设置</button>
                    </form>
                </div>

                <!-- 分隔线 -->
                <hr class="my-4">

                <!-- 背景设置部分 -->
                <div>
                    <h5 class="card-title mb-3">
                        <i class="bi bi-image me-2"></i>背景设置
                    </h5>



                    <form id="backgroundSettingsForm">
                        <div class="form-check mb-3">
                            <input class="form-check-input" type="checkbox" id="enableCustomBackground">
                            <label class="form-check-label" for="enableCustomBackground">
                                启用自定义背景
                            </label>
                        </div>
                        <div class="mb-3">
                            <label for="backgroundImageUrl" class="form-label">背景图片URL</label>
                            <input type="url" class="form-control" id="backgroundImageUrl" placeholder="请输入背景图片URL (必须以https://开头)">
                            <div class="form-text">建议使用高质量图片，支持JPG、PNG格式</div>
                        </div>
                        <div class="mb-3">
                            <label for="pageOpacity" class="form-label">页面透明度: <span id="opacityValue">80</span>%</label>
                            <input type="range" class="form-range" id="pageOpacity" min="0" max="100" value="80" step="1">
                            <div class="form-text">调整页面元素的透明度，数值越小越透明</div>
                        </div>
                        <button type="button" id="saveBackgroundSettingsBtn" class="btn btn-info">保存背景设置</button>
                    </form>
                </div>
            </div>
        </div>
    </div>

    <!-- Global Settings Section (Now integrated above Server Management List) -->
    <!-- The form is now part of the header for Server Management -->
    <!-- End Global Settings Section -->


    <!-- 服务器模态框 -->
    <div class="modal fade" id="serverModal" tabindex="-1">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title" id="serverModalTitle">添加服务器</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <form id="serverForm">
                        <input type="hidden" id="serverId">
                        <div class="mb-3">
                            <label for="serverName" class="form-label">服务器名称</label>
                            <input type="text" class="form-control" id="serverName" required>
                        </div>
                        <div class="mb-3">
                            <label for="serverDescription" class="form-label">描述（可选）</label>
                            <textarea class="form-control" id="serverDescription" rows="2"></textarea>
                        </div>
                        <!-- Removed serverEnableFrequentNotifications checkbox -->

                        <div id="serverIdDisplayGroup" class="mb-3 d-none">
                            <label for="serverIdDisplay" class="form-label">服务器ID</label>
                            <div class="input-group">
                                <input type="text" class="form-control" id="serverIdDisplay" readonly>
                                <button class="btn btn-outline-secondary" type="button" id="copyServerIdBtn">
                                    <i class="bi bi-clipboard"></i>
                                </button>
                            </div>
                        </div>

                        <div id="apiKeyGroup" class="mb-3 d-none">
                            <label for="apiKey" class="form-label">API密钥</label>
                            <div class="input-group">
                                <input type="text" class="form-control" id="apiKey" readonly>
                                <button class="btn btn-outline-secondary" type="button" id="copyApiKeyBtn">
                                    <i class="bi bi-clipboard"></i>
                                </button>
                            </div>
                        </div>

                        <div id="workerUrlDisplayGroup" class="mb-3 d-none">
                            <label for="workerUrlDisplay" class="form-label">Worker 地址</label>
                            <div class="input-group">
                                <input type="text" class="form-control" id="workerUrlDisplay" readonly>
                                <button class="btn btn-outline-secondary" type="button" id="copyWorkerUrlBtn">
                                    <i class="bi bi-clipboard"></i>
                                </button>
                            </div>
                        </div>
                    </form>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">关闭</button>
                    <button type="button" class="btn btn-primary" id="saveServerBtn">保存</button>
                </div>
            </div>
        </div>
    </div>

    <!-- 网站监控模态框 -->
    <div class="modal fade" id="siteModal" tabindex="-1">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title" id="siteModalTitle">添加监控网站</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <form id="siteForm">
                        <input type="hidden" id="siteId">
                        <div class="mb-3">
                            <label for="siteName" class="form-label">网站名称（可选）</label>
                            <input type="text" class="form-control" id="siteName">
                        </div>
                        <div class="mb-3">
                            <label for="siteUrl" class="form-label">网站URL</label>
                            <input type="url" class="form-control" id="siteUrl" placeholder="https://example.com" required>
                        </div>
                        <!-- Removed siteEnableFrequentNotifications checkbox -->
                    </form>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">关闭</button>
                    <button type="button" class="btn btn-primary" id="saveSiteBtn">保存</button>
                </div>
            </div>
        </div>
    </div>

    <!-- 服务器删除确认模态框 -->
    <div class="modal fade" id="deleteModal" tabindex="-1">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title">确认删除</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <p>确定要删除服务器 "<span id="deleteServerName"></span>" 吗？</p>
                    <p class="text-danger">此操作不可逆，所有相关的监控数据也将被删除。</p>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">取消</button>
                    <button type="button" class="btn btn-danger" id="confirmDeleteBtn">删除</button>
                </div>
            </div>
        </div>
    </div>

     <!-- 网站删除确认模态框 -->
    <div class="modal fade" id="deleteSiteModal" tabindex="-1">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title">确认删除网站监控</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <p>确定要停止监控网站 "<span id="deleteSiteName"></span>" (<span id="deleteSiteUrl"></span>) 吗？</p>
                    <p class="text-danger">此操作不可逆。</p>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">取消</button>
                    <button type="button" class="btn btn-danger" id="confirmDeleteSiteBtn">删除</button>
                </div>
            </div>
        </div>
    </div>

    <!-- LLM端点监控模态框 -->
    <div class="modal fade" id="llmEndpointModal" tabindex="-1">
        <div class="modal-dialog modal-lg">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title" id="llmEndpointModalTitle">添加LLM端点</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <form id="llmEndpointForm">
                        <input type="hidden" id="llmEndpointId">
                        <div class="mb-3">
                            <label for="llmEndpointProviderName" class="form-label">Provider 名称（可选）</label>
                            <input type="text" class="form-control" id="llmEndpointProviderName" placeholder="例如：OpenAI 官方 / 自建网关 / SiliconFlow">
                            <div class="form-text">同一个 API URL 下的所有模型会共用这个 Provider 名称；不填写时将自动根据 URL 生成。</div>
                        </div>
                        <div class="mb-3">
                            <label for="llmEndpointModel" class="form-label">模型名称 *</label>
                            <textarea class="form-control" id="llmEndpointModel" rows="3" placeholder="支持多个模型：每行一个、逗号分隔或 JSON 数组均可&#10;例如：gpt-4o-mini, deepseek-chat, claude-sonnet-4-5-20250929" required></textarea>
                            <div class="form-text">支持多个模型：每行一个、逗号分隔或 JSON 数组如 <code>["model-a","model-b"]</code>。同一个 API URL/Key/Prompt/Timeout 会应用到所有模型。</div>
                        </div>
                        <div class="mb-3">
                            <label for="llmEndpointUrl" class="form-label">API URL *</label>
                            <input type="url" class="form-control" id="llmEndpointUrl" placeholder="https://api.openai.com/v1/chat/completions" required>
                        </div>
                        <div class="mb-3">
                            <label for="llmEndpointApiKey" class="form-label">API Key（可选）</label>
                            <input type="password" class="form-control" id="llmEndpointApiKey" placeholder="sk-...">
                        </div>
                        <div class="row">
                            <div class="col-md-6 mb-3">
                                <label for="llmEndpointPrompt" class="form-label">测试 Prompt</label>
                                <input type="text" class="form-control" id="llmEndpointPrompt" placeholder="默认: Say hello in one word">
                            </div>
                            <div class="col-md-6 mb-3">
                                <label for="llmEndpointExpected" class="form-label">期望包含（逗号分隔，可选）</label>
                                <input type="text" class="form-control" id="llmEndpointExpected" placeholder="例如：hello,hi,你好">
                            </div>
                        </div>
                        <div class="mb-3">
                            <label for="llmEndpointTimeout" class="form-label">超时时间 (ms)</label>
                            <input type="number" class="form-control" id="llmEndpointTimeout" value="45000" min="21000" max="120000">
                            <div class="form-text" id="llmEndpointTimeoutHelp">超时时间必须大于当前 LLM 高延迟阈值，建议至少高 1 秒。超过该阈值但请求成功时会标记为“高延迟”状态（仅展示）。</div>
                        </div>
                    </form>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">关闭</button>
                    <button type="button" class="btn btn-primary" id="saveLlmEndpointBtn">保存</button>
                </div>
            </div>
        </div>
    </div>

    <!-- LLM端点删除确认模态框 -->
    <div class="modal fade" id="deleteLlmEndpointModal" tabindex="-1">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title">确认删除LLM端点</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <p>确定要停止监控 LLM 端点 "<span id="deleteLlmEndpointName"></span>" 吗？</p>
                    <p class="text-danger">此操作不可逆。</p>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">取消</button>
                    <button type="button" class="btn btn-danger" id="confirmDeleteLlmEndpointBtn">删除</button>
                </div>
            </div>
        </div>
    </div>

    <!-- 修改密码模态框 -->
    <div class="modal fade" id="passwordModal" tabindex="-1">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title">修改密码</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">

                    <form id="passwordForm">
                        <div class="mb-3">
                            <label for="currentPassword" class="form-label">当前密码</label>
                            <input type="password" class="form-control" id="currentPassword" required>
                        </div>
                        <div class="mb-3">
                            <label for="newPassword" class="form-label">新密码</label>
                            <input type="password" class="form-control" id="newPassword" required>
                        </div>
                        <div class="mb-3">
                            <label for="confirmPassword" class="form-label">确认新密码</label>
                            <input type="password" class="form-control" id="confirmPassword" required>
                        </div>
                    </form>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">取消</button>
                    <button type="button" class="btn btn-primary" id="savePasswordBtn">保存</button>
                </div>
            </div>
        </div>
    </div>

    <footer class="footer fixed-bottom py-2 bg-light border-top">
        <div class="container text-center">
            <span class="text-muted small">VPS监控面板 &copy; 2025</span>
            <a href="https://github.com/kadidalax/cf-vps-monitor" target="_blank" rel="noopener noreferrer" class="ms-3 text-muted" title="GitHub Repository">
                <i class="bi bi-github"></i>
            </a>
        </div>
    </footer>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js" integrity="sha384-C6RzsynM9kWDrMNeT87bh95OGNyZPhcTNXj1NW7RuBCsyN/o0jlpcV8Qyq46cDfL" crossorigin="anonymous"></script>
    <script src="/js/admin.js"></script>
</body>
</html>`;
}

function getFaviconSvg() {
  return `<svg width="32" height="32" viewBox="0 0 32 32" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <radialGradient id="bg" cx="0.3" cy="0.3">
      <stop offset="0%" stop-color="#fff" stop-opacity="0.9"/>
      <stop offset="100%" stop-color="#0277bd" stop-opacity="0.8"/>
    </radialGradient>
    <linearGradient id="ecg" x1="0%" x2="100%">
      <stop offset="0%" stop-color="#f08"/>
      <stop offset="50%" stop-color="#0f8"/>
      <stop offset="100%" stop-color="#80f"/>
    </linearGradient>
  </defs>
  <circle cx="16" cy="16" r="15" fill="url(#bg)" stroke="#0277bd" stroke-width="1.5"/>
  <circle cx="16" cy="16" r="13" fill="none" stroke="#fff" stroke-width="1" opacity="0.4"/>
  <line x1="4" y1="16" x2="28" y2="16" stroke="#b3e5fc" stroke-width="0.5" opacity="0.8"/>
  <path id="p" d="M4 16L8 16L9 15L10 17L11 14L12 18L13 10L14 22L15 16L28 16" fill="none" stroke="url(#ecg)" stroke-width="2.8"/>
  <path d="M4 16L8 16L9 15L10 17L11 14L12 18L13 10L14 22L15 16L28 16" fill="none" stroke="#fff" stroke-width="1.2" opacity="0.7"/>
  <circle r="1.5" fill="#fff">
    <animateMotion dur="2s" repeatCount="indefinite">
      <mpath href="#p"/>
    </animateMotion>
  </circle>
  <circle cx="16" cy="16" r="8" fill="none" stroke="#f08" stroke-width="0.5" opacity="0.6">
    <animate attributeName="r" values="8;12;8" dur="3s" repeatCount="indefinite"/>
    <animate attributeName="opacity" values="0.6;0;0.6" dur="3s" repeatCount="indefinite"/>
  </circle>
</svg>`;
}

function getStyleCss() {
  return `/* 全局样式 */
body {
    min-height: 100vh;
    display: flex;
    flex-direction: column;
}

.footer {
    margin-top: auto;
}

/* 图表容器 */
.chart-container {
    position: relative;
    height: 200px;
    width: 100%;
}

/* 卡片样式 */
.card {
    box-shadow: 0 0.125rem 0.25rem rgba(0, 0, 0, 0.075);
    margin-bottom: 1.5rem;
}

.card-header {
    background-color: rgba(0, 0, 0, 0.03);
    border-bottom: 1px solid rgba(0, 0, 0, 0.125);
}

/* 进度条样式 */
.progress {
    height: 0.75rem;
}

/* 表格样式 */
.table th {
    font-weight: 600;
}

/* Modal centering and light theme transparency */
.modal-dialog {
    display: flex;
    align-items: center;
    min-height: calc(100% - 1rem); /* Adjust as needed */
}

.modal-content {
    background-color: rgba(255, 255, 255, 0.9); /* Semi-transparent white for light theme */
    /* backdrop-filter: blur(5px); /* Optional: adds a blur effect to content behind modal */
}


/* 响应式调整 */
@media (max-width: 768px) {
    .chart-container {
        height: 150px;
    }

    /* 移动端隐藏表格，显示卡片 */
    .table-responsive {
        display: none !important;
    }

    .mobile-card-container {
        display: block !important;
    }

    /* 移动端隐藏桌面端按钮 */
    .desktop-only {
        display: none !important;
    }

    /* 移动端导航栏优化 */
    .navbar-brand {
        font-size: 1rem;
        margin-right: 0.5rem;
    }

    .container {
        padding-left: 10px;
        padding-right: 10px;
    }

    /* 移动端导航栏按钮组优化 */
    .navbar .d-flex {
        gap: 0.25rem;
        flex-wrap: wrap;
    }

    .navbar .btn-sm {
        padding: 0.25rem 0.5rem;
        font-size: 0.75rem;
        min-width: auto;
        border-width: 1px;
    }

    .navbar .nav-link {
        font-size: 0.8rem;
        padding: 0.25rem 0.5rem;
        margin: 0;
    }

    /* 移动端导航栏下拉菜单优化 - 精简版 */
    .dropdown-menu {
        font-size: 0.875rem;
        min-width: 150px;
        z-index: 10000 !important; /* 统一使用最高层级 */
        position: absolute !important; /* 使用absolute定位确保正确显示 */
        /* 移除position: fixed，让Bootstrap自动处理定位 */
    }

    /* 确保导航栏有合适的层级但不创建层叠上下文 */
    .navbar {
        position: relative;
        z-index: 1000; /* 给导航栏一个中等层级 */
    }

    .navbar .dropdown-item {
        padding: 0.5rem 1rem;
        font-size: 0.875rem;
    }

    .navbar .dropdown-item i {
        width: 1.2rem;
    }

    /* 移动端管理区域标题行优化 */
    .admin-header-row {
        display: flex;
        flex-direction: column;
        gap: 0.75rem; /* 减少移动端间隔 */
    }

    .admin-header-title h2 {
        font-size: 1.5rem;
        margin-bottom: 0;
    }

    .admin-header-content {
        display: flex;
        flex-direction: column;
        gap: 0.5rem; /* 减少移动端间隔 */
    }

    .admin-settings-form {
        order: 2; /* 设置表单在移动端显示在按钮组下方 */
    }

    .admin-actions-group {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        order: 1; /* 按钮组在移动端显示在上方 */
    }

    .settings-group {
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
    }

    .settings-group .form-label {
        font-size: 0.875rem;
        margin-bottom: 0;
        font-weight: 500;
    }

    .settings-group .input-group {
        max-width: 250px;
    }

    /* 超小屏幕优化 (小于400px) */
    @media (max-width: 400px) {
        .navbar-brand {
            font-size: 0.9rem;
        }

        .navbar .btn-sm {
            padding: 0.2rem 0.4rem;
            font-size: 0.7rem;
        }

        .navbar .nav-link {
            font-size: 0.75rem;
            padding: 0.2rem 0.4rem;
        }

        .container {
            padding-left: 8px;
            padding-right: 8px;
        }
    }

    /* 移动端按钮优化 */
    .btn-sm {
        padding: 0.375rem 0.75rem;
        font-size: 0.875rem;
    }
}

/* 桌面端隐藏卡片容器和移动端菜单 */
@media (min-width: 769px) {
    .mobile-card-container {
        display: none !important;
    }

    .mobile-only {
        display: none !important;
    }
}

    /* 桌面端管理区域标题行样式 */
    .admin-header-row {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        flex-wrap: wrap;
        gap: 0.75rem; /* 减少桌面端间隔 */
    }

    .admin-header-title {
        flex: 0 0 auto;
    }

    .admin-header-content {
        display: flex;
        align-items: center;
        gap: 1rem;
        flex: 1 1 auto;
        justify-content: flex-end;
    }

    .admin-settings-form {
        order: 1;
        margin-right: auto; /* 推送到左侧 */
    }

    .admin-actions-group {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        order: 2;
    }

    .settings-group {
        display: flex;
        flex-direction: row;
        align-items: center;
        gap: 0.5rem;
    }

    .settings-group .form-label {
        margin-bottom: 0;
        white-space: nowrap;
        font-size: 0.875rem;
    }
}

/* 单一卡片布局样式 */
.card.shadow-sm {
    border: none;
    box-shadow: 0 0.125rem 0.5rem rgba(0, 0, 0, 0.1) !important;
}

.card-title {
    color: var(--bs-primary);
    font-weight: 600;
}

.card-title i {
    color: var(--bs-primary);
}

/* 分隔线样式 */
hr.my-4 {
    border-color: var(--bs-border-color-translucent);
    opacity: 0.5;
}

/* 暗色主题下的单一卡片样式 */
[data-bs-theme="dark"] .card.shadow-sm {
    background-color: var(--bs-dark);
    box-shadow: 0 0.125rem 0.5rem rgba(0, 0, 0, 0.3) !important;
}

[data-bs-theme="dark"] .card-title {
    color: #86b7fe;
}

[data-bs-theme="dark"] .card-title i {
    color: #86b7fe;
}

/* VPS监控面板标题 - 蓝色加粗 */
.navbar-brand {
    color: var(--bs-primary) !important;
    font-weight: 600 !important;
}
[data-bs-theme="dark"] .navbar-brand {
    color: #86b7fe !important;
}

/* 导航栏主题跟随 - 精简版 */
[data-bs-theme="light"] .navbar { background-color: #f8f9fa !important; }
[data-bs-theme="dark"] .navbar { background-color: #212529 !important; }

/* 导航栏文字主题跟随 */
[data-bs-theme="light"] .navbar .nav-link, [data-bs-theme="light"] .navbar a { color: #212529 !important; }
[data-bs-theme="dark"] .navbar .nav-link, [data-bs-theme="dark"] .navbar a { color: #ffffff !important; }

/* 导航栏按钮主题跟随 */
[data-bs-theme="light"] .navbar .btn-outline-light { border-color: #212529 !important; color: #212529 !important; }
[data-bs-theme="dark"] .navbar .btn-outline-light { border-color: #ffffff !important; color: #ffffff !important; }

/* 导航栏图标主题跟随 */
[data-bs-theme="light"] .navbar i { color: #212529 !important; }
[data-bs-theme="dark"] .navbar i { color: #ffffff !important; }

/* 底部版权信息 - 主题跟随调大 */
.footer .text-muted { font-size: 0.95rem !important; font-weight: 500; }
.footer a.text-muted { font-size: 1.1rem !important; }
.footer .text-muted { color: #212529 !important; }
[data-bs-theme="dark"] .footer .text-muted { color: #ffffff !important; }

[data-bs-theme="dark"] hr.my-4 {
    border-color: rgba(255, 255, 255, 0.2);
}

/* 固定底部页脚样式 */
body {
    padding-bottom: 60px; /* 为固定页脚留出空间 */
}

.footer.fixed-bottom {
    height: 35px;
    background-color: var(--bs-light) !important;
    border-top: 1px solid var(--bs-border-color);
    display: flex;
    align-items: center;
}

/* 暗色主题下的页脚 */
[data-bs-theme="dark"] .footer.fixed-bottom {
    background-color: var(--bs-dark) !important;
    border-top-color: var(--bs-border-color);
}

/* 移动端卡片样式 */
.mobile-card-container {
    display: none; /* 默认隐藏，通过媒体查询控制 */
    position: relative;
    z-index: 0; /* 降低容器层级，确保下拉菜单在上方 */
}

.mobile-server-card, .mobile-site-card {
    background: var(--bs-card-bg, #fff);
    border: 1px solid var(--bs-border-color, rgba(0,0,0,.125));
    border-radius: 0.5rem;
    margin-bottom: 0.75rem;
    box-shadow: 0 0.125rem 0.25rem rgba(0, 0, 0, 0.075);
    overflow: hidden;
    transition: box-shadow 0.15s ease-in-out, transform 0.15s ease-in-out;
    position: relative;
    z-index: 0; /* 降低卡片层级，确保下拉菜单在上方 */
}

@media (max-width: 768px) {
    .mobile-server-card:hover, .mobile-site-card:hover {
        box-shadow: 0 0.25rem 0.5rem rgba(0, 0, 0, 0.1);
    }
}

.mobile-card-header {
    padding: 0.75rem;
    background-color: var(--bs-card-cap-bg, rgba(0,0,0,.03));
    border-bottom: 1px solid var(--bs-border-color, rgba(0,0,0,.125));
    display: flex;
    justify-content: space-between;
    align-items: center;
    position: relative;
    z-index: 0; /* 降低卡片头部层级，确保下拉菜单在上方 */
}

.mobile-card-header-left {
    flex: 0 0 auto;
}

.mobile-card-header-right {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    font-size: 0.875rem;
}

.mobile-card-footer {
    margin-top: 0.5rem;
    padding-top: 0.5rem;
    border-top: 1px solid var(--bs-border-color, rgba(0,0,0,.125));
    font-size: 0.875rem;
    color: var(--bs-secondary);
}

@media (max-width: 768px) {
    .mobile-card-header:hover {
        background-color: var(--bs-card-cap-bg, rgba(0,0,0,.05));
    }
}

.mobile-card-title {
    font-weight: 600;
    margin: 0;
    font-size: 1rem;
    line-height: 1.3;
}

.mobile-card-status {
    flex-shrink: 0;
}

.mobile-card-body {
    padding: 0.75rem;
}

.mobile-card-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.5rem 0;
    border-bottom: 1px solid var(--bs-border-color-translucent, rgba(0,0,0,.08));
}

.mobile-card-row:last-child {
    border-bottom: none;
    padding-bottom: 0;
}

/* 两列布局样式 */
.mobile-card-two-columns {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.75rem;
    padding: 0.5rem 0;
    border-bottom: 1px solid var(--bs-border-color-translucent, rgba(0,0,0,.08));
}

.mobile-card-two-columns:last-child {
    border-bottom: none;
    padding-bottom: 0.25rem;
}

.mobile-card-column-item {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
    min-height: 2rem;
    justify-content: center;
}

.mobile-card-column-item .mobile-card-label {
    font-size: 0.7rem;
    margin-bottom: 0;
    color: var(--bs-secondary-color, #6c757d);
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.02em;
}

.mobile-card-column-item .mobile-card-value {
    font-size: 0.85rem;
    font-weight: 600;
    text-align: left;
    max-width: 100%;
    word-break: break-word;
    line-height: 1.2;
}

/* 移动端单行样式优化 */
@media (max-width: 768px) {
    .mobile-card-row {
        padding: 0.5rem 0;
        min-height: 2rem;
        align-items: center;
    }

    .mobile-card-label {
        font-weight: 500;
        font-size: 0.875rem;
    }

    .mobile-card-value {
        font-weight: 600;
        font-size: 0.875rem;
        word-break: break-word;
    }
}

.mobile-card-label {
    font-weight: 500;
    color: var(--bs-secondary-color, #6c757d);
    font-size: 0.875rem;
}

.mobile-card-value {
    text-align: right;
    flex-shrink: 0;
    max-width: 60%;
}



/* 移动端进度条优化 */
@media (max-width: 768px) {
    .progress {
        height: 1rem;
        margin-top: 0.25rem;
        border-radius: 0.5rem;
    }

    .progress span {
        font-size: 0.75rem;
        line-height: 1rem;
    }
}

/* 移动端状态徽章优化 */
@media (max-width: 768px) {
    .badge {
        font-size: 0.75rem;
        padding: 0.35em 0.65em;
        border-radius: 0.375rem;
    }
}

/* 移动端历史记录条优化 */
@media (max-width: 768px) {
    .mobile-history-container .history-bar-container {
        height: 1.5rem;
        border-radius: 0.25rem;
        overflow: hidden;
        display: flex;
        width: 100%;
        gap: 1px;
    }

    .mobile-history-container .history-bar {
        flex: 1;
        min-width: 0;
        border-radius: 1px;
        height: 100%;
    }
}

/* 移动端历史记录条优化 */
.mobile-history-container {
    margin-top: 0.5rem;
}

.mobile-history-label {
    font-size: 0.75rem;
    color: var(--bs-secondary-color, #6c757d);
    margin-bottom: 0.25rem;
}



/* 移动端按钮优化 */
@media (max-width: 768px) {
    .mobile-card-body .btn-sm {
        padding: 0.5rem 0.75rem;
        font-size: 0.8rem;
        border-radius: 0.375rem;
        transition: all 0.15s ease-in-out;
    }

    .mobile-card-body .d-flex.gap-2 {
        gap: 0.5rem !important;
    }

    .mobile-card-body .btn i {
        font-size: 0.875rem;
    }

    /* 移动端触摸反馈 */
    .mobile-card-header:active {
        background-color: var(--bs-card-cap-bg, rgba(0,0,0,.08)) !important;
    }

    .mobile-card-body .btn:active {
        opacity: 0.8;
    }

    /* 移动端容器标题优化 */
    .container h2 {
        font-size: 1.5rem;
        margin-bottom: 1rem;
    }

    /* 移动端卡片标题层次优化 */
    .mobile-card-title {
        font-size: 1rem;
        line-height: 1.3;
        font-weight: 600;
    }

    /* 移动端管理页面按钮优化 */
    .admin-actions-group .btn {
        font-size: 0.875rem;
        padding: 0.5rem 0.75rem;
        border-radius: 0.375rem;
        transition: all 0.2s ease-in-out;
    }

    .admin-actions-group .btn:active {
        transform: scale(0.95);
    }

    .admin-actions-group .dropdown-toggle {
        min-width: auto;
    }



    /* 移动端卡片间距优化 */
    .mobile-server-card, .mobile-site-card {
        margin-bottom: 1rem;
    }

    .mobile-card-body {
        padding: 0.75rem;
    }

    .mobile-card-row {
        padding: 0.375rem 0;
        border-bottom: 1px solid var(--bs-border-color-translucent, rgba(0,0,0,.08));
    }

    .mobile-card-row:last-child {
        border-bottom: none;
    }
}

/* 自定义浅绿色进度条 */
.bg-light-green {
    background-color: #90ee90 !important; /* LightGreen */
}

/* Custom styles for non-disruptive alerts in admin page */
#serverAlert, #siteAlert, #telegramSettingsAlert, #ntfySettingsAlert {
    position: fixed !important; /* Use !important to override Bootstrap if necessary */
    top: 70px; /* Below navbar */
    left: 50%;
    transform: translateX(-50%);
    z-index: 1055; /* Higher than Bootstrap modals (1050) */
    padding: 0.75rem 1.25rem;
    /* margin-bottom: 1rem; /* Not needed for fixed */
    border: 1px solid transparent;
    border-radius: 0.25rem;
    min-width: 300px; /* Minimum width */
    max-width: 90%; /* Max width */
    text-align: center;
    box-shadow: 0 0.5rem 1rem rgba(0,0,0,0.15);
    /* Ensure d-none works to hide them, !important might be needed if Bootstrap's .alert.d-none is too specific */
}

#serverAlert.d-none, #siteAlert.d-none, #telegramSettingsAlert.d-none, #ntfySettingsAlert.d-none {
    display: none !important;
}

/* Semi-transparent backgrounds for different alert types */
/* Light Theme Overrides for fixed alerts */
#serverAlert.alert-success, #siteAlert.alert-success, #telegramSettingsAlert.alert-success, #ntfySettingsAlert.alert-success {
    color: #0f5132; /* Bootstrap success text color */
    background-color: rgba(209, 231, 221, 0.95) !important; /* Semi-transparent success, !important for specificity */
    border-color: rgba(190, 221, 208, 0.95) !important;
}

#serverAlert.alert-danger, #siteAlert.alert-danger, #telegramSettingsAlert.alert-danger, #ntfySettingsAlert.alert-danger {
    color: #842029; /* Bootstrap danger text color */
    background-color: rgba(248, 215, 218, 0.95) !important; /* Semi-transparent danger */
    border-color: rgba(245, 198, 203, 0.95) !important;
}

#serverAlert.alert-warning, #siteAlert.alert-warning, #telegramSettingsAlert.alert-warning, #ntfySettingsAlert.alert-warning { /* For siteAlert if it uses warning */
    color: #664d03; /* Bootstrap warning text color */
    background-color: rgba(255, 243, 205, 0.95) !important; /* Semi-transparent warning */
    border-color: rgba(255, 238, 186, 0.95) !important;
}


    [data-bs-theme="dark"] {
        body {
            background-color: #121212; /* 深色背景 */
            color: #e0e0e0; /* 浅色文字 */
        }

        .card {
            background-color: #1e1e1e; /* 卡片深色背景 */
            border: 1px solid #333;
            color: #e0e0e0; /* 卡片内文字颜色 */
        }

        .card-header {
            background-color: #2a2a2a;
            border-bottom: 1px solid #333;
            color: #f5f5f5;
        }

        .table {
            color: #e0e0e0; /* 表格文字颜色 */
        }

        .table th, .table td {
            border-color: #333; /* 表格边框颜色 */
        }

        .table-striped > tbody > tr:nth-of-type(odd) > * {
             background-color: rgba(255, 255, 255, 0.05); /* 深色模式下的条纹 */
             color: #e0e0e0;
        }

        .table-hover > tbody > tr:hover > * {
            background-color: rgba(255, 255, 255, 0.075); /* 深色模式下的悬停 */
            color: #f0f0f0;
        }

        .modal-content {
            background-color: rgba(30, 30, 30, 0.9); /* Semi-transparent dark grey for dark theme */
            color: #e0e0e0;
            /* backdrop-filter: blur(5px); /* Optional: adds a blur effect to content behind modal */
        }

        .modal-header {
            border-bottom-color: #333;
        }

        .modal-footer {
            border-top-color: #333;
        }

        .form-control {
            background-color: #2a2a2a;
            color: #e0e0e0;
            border-color: #333;
        }

        .form-control:focus {
            background-color: #2a2a2a;
            color: #e0e0e0;
            border-color: #555;
            box-shadow: 0 0 0 0.25rem rgba(100, 100, 100, 0.25);
        }

        .btn-outline-secondary {
             color: #adb5bd;
             border-color: #6c757d;
        }
        .btn-outline-secondary:hover {
             color: #fff;
             background-color: #6c757d;
             border-color: #6c757d;
        }

        .navbar {
            background-color: #1e1e1e !important; /* 确保覆盖 Bootstrap 默认 */
        }

        /* 暗色主题移动端卡片样式 */
        .mobile-server-card, .mobile-site-card {
            background: var(--bs-dark, #212529);
            border-color: var(--bs-border-color, #495057);
        }

        .mobile-card-header {
            background-color: rgba(255, 255, 255, 0.05);
            border-bottom-color: var(--bs-border-color, #495057);
        }

        .mobile-card-title {
            color: #ffffff !important;
        }

        .mobile-card-label {
            color: #ced4da !important;
        }

        .mobile-card-value {
            color: #ffffff !important;
        }



        .mobile-card-row {
            border-bottom-color: rgba(255, 255, 255, 0.08);
        }

        .mobile-card-two-columns {
            border-bottom-color: rgba(255, 255, 255, 0.08);
        }

        .mobile-card-column-item .mobile-card-label {
            color: #ced4da !important;
        }

        .mobile-card-column-item .mobile-card-value {
            color: #ffffff !important;
        }

        .mobile-history-label {
            color: #ced4da !important;
        }

        /* 暗色主题下的空状态和错误状态文字 */
        .mobile-card-container .text-muted {
            color: #ced4da !important;
        }

        .mobile-card-container .text-danger {
            color: #ff6b6b !important;
        }

        .mobile-card-container h6 {
            color: #ffffff !important;
        }

        .mobile-card-container small {
            color: #adb5bd !important;
        }

        /* 暗色主题下的移动端按钮优化 */
        .mobile-card-body .btn-outline-primary {
            color: #6ea8fe !important;
            border-color: #6ea8fe !important;
        }

        .mobile-card-body .btn-outline-primary:hover {
            color: #000 !important;
            background-color: #6ea8fe !important;
            border-color: #6ea8fe !important;
        }

        .mobile-card-body .btn-outline-info {
            color: #6edff6 !important;
            border-color: #6edff6 !important;
        }

        .mobile-card-body .btn-outline-info:hover {
            color: #000 !important;
            background-color: #6edff6 !important;
            border-color: #6edff6 !important;
        }

        .mobile-card-body .btn-outline-danger {
            color: #ea868f !important;
            border-color: #ea868f !important;
        }

        .mobile-card-body .btn-outline-danger:hover {
            color: #000 !important;
            background-color: #ea868f !important;
            border-color: #ea868f !important;
        }

        /* 暗色主题下的Badge徽章优化 */
        .mobile-card-header .badge.bg-success {
            background-color: #198754 !important;
            color: #ffffff !important;
        }

        .mobile-card-header .badge.bg-danger {
            background-color: #dc3545 !important;
            color: #ffffff !important;
        }

        .mobile-card-header .badge.bg-warning {
            background-color: #ffc107 !important;
            color: #000000 !important;
        }

        .mobile-card-header .badge.bg-secondary {
            background-color: #6c757d !important;
            color: #ffffff !important;
        }

        .mobile-card-header .badge.bg-primary {
            background-color: #0d6efd !important;
            color: #ffffff !important;
        }

        /* 暗色主题下的移动端容器标题优化 */
        .container h2 {
            color: #ffffff !important;
        }

        /* 暗色主题下的移动端加载状态优化 */
        .mobile-card-container .spinner-border {
            color: #6ea8fe !important;
        }

        .mobile-card-container .mt-2 {
            color: #ced4da !important;
        }

        /* 暗色主题下的导航栏按钮优化 */
        .navbar .btn-outline-light {
            color: #f8f9fa !important;
            border-color: #f8f9fa !important;
        }

        .navbar .btn-outline-light:hover {
            color: #000 !important;
            background-color: #f8f9fa !important;
            border-color: #f8f9fa !important;
        }

        .navbar .nav-link {
            color: #f8f9fa !important;
        }

        .navbar .nav-link:hover {
            color: #e9ecef !important;
        }
        .navbar-light .navbar-nav .nav-link {
             color: #ccc;
        }
        .navbar-light .navbar-nav .nav-link:hover {
             color: #fff;
        }
        .navbar-light .navbar-brand {
             color: #fff;
        }
         .footer {
            background-color: #1e1e1e !important;
            color: #cccccc; /* 修复夜间模式页脚文本颜色 */
        }
        a {
            color: #8ab4f8; /* 示例链接颜色 */
        }
        a:hover {
            color: #a9c9fc;
        }

        /* Dark Theme Overrides for fixed alerts */
        [data-bs-theme="dark"] #serverAlert.alert-success,
        [data-bs-theme="dark"] #siteAlert.alert-success,
        [data-bs-theme="dark"] #telegramSettingsAlert.alert-success,
        [data-bs-theme="dark"] #ntfySettingsAlert.alert-success {
            color: #75b798; /* Lighter green text for dark theme */
            background-color: rgba(40, 167, 69, 0.85) !important; /* Darker semi-transparent success */
            border-color: rgba(34, 139, 57, 0.85) !important;
        }

        [data-bs-theme="dark"] #serverAlert.alert-danger,
        [data-bs-theme="dark"] #siteAlert.alert-danger,
        [data-bs-theme="dark"] #telegramSettingsAlert.alert-danger,
        [data-bs-theme="dark"] #ntfySettingsAlert.alert-danger {
            color: #ea868f; /* Lighter red text for dark theme */
            background-color: rgba(220, 53, 69, 0.85) !important; /* Darker semi-transparent danger */
            border-color: rgba(187, 45, 59, 0.85) !important;
        }

        [data-bs-theme="dark"] #serverAlert.alert-warning,
        [data-bs-theme="dark"] #siteAlert.alert-warning,
        [data-bs-theme="dark"] #telegramSettingsAlert.alert-warning,
        [data-bs-theme="dark"] #ntfySettingsAlert.alert-warning {
            color: #ffd373; /* Lighter yellow text for dark theme */
            background-color: rgba(255, 193, 7, 0.85) !important; /* Darker semi-transparent warning */
            border-color: rgba(217, 164, 6, 0.85) !important;
        }
    }

/* 拖拽排序样式 */
.server-row-draggable, .site-row-draggable {
    transition: all 0.2s ease;
}
.server-row-draggable:hover, .site-row-draggable:hover {
    background-color: rgba(0, 123, 255, 0.1) !important;
}
.server-row-draggable.drag-over-top, .site-row-draggable.drag-over-top {
    border-top: 3px solid #007bff !important;
    background-color: rgba(0, 123, 255, 0.1) !important;
}
.server-row-draggable.drag-over-bottom, .site-row-draggable.drag-over-bottom {
    border-bottom: 3px solid #007bff !important;
    background-color: rgba(0, 123, 255, 0.1) !important;
}
.server-row-draggable[draggable="true"], .site-row-draggable[draggable="true"] {
    cursor: grab;
}
.server-row-draggable[draggable="true"]:active, .site-row-draggable[draggable="true"]:active {
    cursor: grabbing;
}

/* 暗色主题下的拖拽样式 */
[data-bs-theme="dark"] .server-row-draggable:hover,
[data-bs-theme="dark"] .site-row-draggable:hover {
    background-color: rgba(13, 110, 253, 0.2) !important;
}
[data-bs-theme="dark"] .server-row-draggable.drag-over-top,
[data-bs-theme="dark"] .site-row-draggable.drag-over-top {
    border-top: 3px solid #0d6efd !important;
    background-color: rgba(13, 110, 253, 0.2) !important;
}
[data-bs-theme="dark"] .server-row-draggable.drag-over-bottom,
[data-bs-theme="dark"] .site-row-draggable.drag-over-bottom {
    border-bottom: 3px solid #0d6efd !important;
    background-color: rgba(13, 110, 253, 0.2) !important;
}

/* ==================== 自定义背景和透明度控制系统 ==================== */

/* CSS变量定义 */
:root {
    --custom-background-url: '';
    --page-opacity: 0.8;
    --text-contrast-light: rgba(0, 0, 0, 0.87);
    --text-contrast-dark: rgba(255, 255, 255, 0.87);
    --background-overlay-light: rgba(255, 255, 255, 0.9);
    --background-overlay-dark: rgba(18, 18, 18, 0.9);
}

/* 背景图片显示 */
body.custom-background-enabled::before {
    content: '';
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background-image: var(--custom-background-url);
    background-size: cover;
    background-position: center;
    background-repeat: no-repeat;
    background-attachment: fixed;
    z-index: -1;
    opacity: 1;
}

/* 启用自定义背景时的页面元素透明度调整 */
body.custom-background-enabled .navbar {
    background-color: rgba(248, 249, 250, var(--page-opacity)) !important;
    /* 移除导航栏的backdrop-filter，避免影响下拉菜单层级 */
    /* backdrop-filter: blur(10px); */
    /* -webkit-backdrop-filter: blur(10px); */
}

body.custom-background-enabled .card {
    background-color: rgba(255, 255, 255, var(--page-opacity)) !important;
    /* 移除大卡片的backdrop-filter，避免创建层叠上下文影响下拉菜单 */
    /* backdrop-filter: blur(5px); */
    /* -webkit-backdrop-filter: blur(5px); */
    border: 1px solid rgba(0, 0, 0, 0.125);
}

body.custom-background-enabled .card-header {
    background-color: rgba(0, 0, 0, calc(0.03 * var(--page-opacity))) !important;
    border-bottom: 1px solid rgba(0, 0, 0, calc(0.125 * var(--page-opacity)));
}

body.custom-background-enabled .modal-content {
    background-color: rgba(255, 255, 255, var(--page-opacity)) !important;
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
}

body.custom-background-enabled .footer {
    background-color: rgba(248, 249, 250, var(--page-opacity)) !important;
    backdrop-filter: blur(5px);
    -webkit-backdrop-filter: blur(5px);
}

/* 表格透明度调整 - 避免与卡片背景叠加 */
body.custom-background-enabled .table {
    background-color: transparent !important;
}

body.custom-background-enabled .table th {
    background-color: transparent !important;
    backdrop-filter: none;
    -webkit-backdrop-filter: none;
}

body.custom-background-enabled .table td {
    background-color: transparent !important;
}

/* 输入框完全透明化 - 方案A */
body.custom-background-enabled .form-control {
    background-color: transparent !important;
    backdrop-filter: none;
    -webkit-backdrop-filter: none;
    border: 1px solid rgba(0, 0, 0, 0.15) !important;
}

body.custom-background-enabled .form-control:focus {
    background-color: transparent !important;
    border: 1px solid rgba(13, 110, 253, 0.6) !important;
    box-shadow: 0 0 0 0.25rem rgba(13, 110, 253, 0.15) !important;
}

/* 按钮透明度调整 */
body.custom-background-enabled .btn {
    backdrop-filter: blur(3px);
    -webkit-backdrop-filter: blur(3px);
}

/* 滑块完全透明化 - 完整重置 */
body.custom-background-enabled .form-range {
    -webkit-appearance: none !important;
    appearance: none !important;
    background: transparent !important;
    outline: none !important;
}

/* WebKit浏览器 (Chrome, Safari) */
body.custom-background-enabled .form-range::-webkit-slider-track {
    -webkit-appearance: none !important;
    appearance: none !important;
    background: transparent !important;
    border: 1px solid rgba(0, 0, 0, 0.15) !important;
    height: 6px !important;
    border-radius: 3px !important;
    box-shadow: none !important;
    outline: none !important;
    margin: 0 !important;
    padding: 0 !important;
    box-sizing: border-box !important;
}

body.custom-background-enabled .form-range::-webkit-slider-runnable-track {
    -webkit-appearance: none !important;
    background: transparent !important;
    border: 1px solid rgba(0, 0, 0, 0.15) !important;
    height: 6px !important;
    border-radius: 3px !important;
    box-shadow: none !important;
}

/* Firefox */
body.custom-background-enabled .form-range::-moz-range-track {
    background: transparent !important;
    border: 1px solid rgba(0, 0, 0, 0.15) !important;
    height: 6px !important;
    border-radius: 3px !important;
    box-shadow: none !important;
    outline: none !important;
}

body.custom-background-enabled .form-range::-moz-range-progress {
    background: transparent !important;
    height: 6px !important;
    border-radius: 3px !important;
}

/* 滑块按钮 - 垂直居中对齐 */
body.custom-background-enabled .form-range::-webkit-slider-thumb {
    -webkit-appearance: none !important;
    appearance: none !important;
    background-color: rgba(13, 110, 253, 0.8) !important;
    border: 1px solid rgba(0, 0, 0, 0.1) !important;
    width: 20px !important;
    height: 20px !important;
    border-radius: 50% !important;
    cursor: pointer !important;
    margin-top: -7px !important;
    box-sizing: border-box !important;
}

body.custom-background-enabled .form-range::-moz-range-thumb {
    background-color: rgba(13, 110, 253, 0.8) !important;
    border: 1px solid rgba(0, 0, 0, 0.1) !important;
    width: 20px !important;
    height: 20px !important;
    border-radius: 50% !important;
    cursor: pointer !important;
    box-shadow: none !important;
    margin-top: -8px !important;
    box-sizing: border-box !important;
}

/* 下拉菜单透明度调整 - 确保最高层级显示 */
body.custom-background-enabled .dropdown-menu {
    background-color: rgba(255, 255, 255, var(--page-opacity)) !important;
    /* 移除backdrop-filter避免创建层叠上下文，确保z-index正常工作 */
    /* backdrop-filter: blur(5px); */
    /* -webkit-backdrop-filter: blur(5px); */
}

/* 移动端卡片透明度调整 - 移除backdrop-filter避免创建层叠上下文 */
body.custom-background-enabled .mobile-server-card,
body.custom-background-enabled .mobile-site-card {
    background-color: rgba(255, 255, 255, var(--page-opacity)) !important;
    /* backdrop-filter: blur(5px); 注释掉以避免创建层叠上下文遮挡下拉菜单 */
    /* -webkit-backdrop-filter: blur(5px); */
}

body.custom-background-enabled .mobile-card-header {
    background-color: rgba(0, 0, 0, calc(0.03 * var(--page-opacity))) !important;
}

/* 表格条纹和悬停效果 - 轻微背景色，不叠加透明度 */
body.custom-background-enabled .table-striped > tbody > tr:nth-of-type(odd) > * {
    background-color: rgba(0, 0, 0, 0.02) !important;
}

body.custom-background-enabled .table-hover > tbody > tr:hover > * {
    background-color: rgba(0, 0, 0, 0.04) !important;
}

/* 暗色主题下的自定义背景样式 */
[data-bs-theme="dark"] body.custom-background-enabled .navbar {
    background-color: rgba(30, 30, 30, var(--page-opacity)) !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .card {
    background-color: rgba(30, 30, 30, var(--page-opacity)) !important;
    border-color: rgba(51, 51, 51, var(--page-opacity));
}

[data-bs-theme="dark"] body.custom-background-enabled .card-header {
    background-color: rgba(42, 42, 42, var(--page-opacity)) !important;
    border-bottom-color: rgba(51, 51, 51, var(--page-opacity));
}

[data-bs-theme="dark"] body.custom-background-enabled .modal-content {
    background-color: rgba(30, 30, 30, var(--page-opacity)) !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .footer {
    background-color: rgba(30, 30, 30, var(--page-opacity)) !important;
}

/* 暗色主题下的表格透明度调整 - 避免与卡片背景叠加 */
[data-bs-theme="dark"] body.custom-background-enabled .table {
    background-color: transparent !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .table th {
    background-color: transparent !important;
    backdrop-filter: none;
    -webkit-backdrop-filter: none;
}

[data-bs-theme="dark"] body.custom-background-enabled .table td {
    background-color: transparent !important;
}

/* 暗色主题下的输入框完全透明化 - 方案A */
[data-bs-theme="dark"] body.custom-background-enabled .form-control {
    background-color: transparent !important;
    backdrop-filter: none;
    -webkit-backdrop-filter: none;
    border: 1px solid rgba(255, 255, 255, 0.2) !important;
    color: rgba(255, 255, 255, 0.9) !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .form-control:focus {
    background-color: transparent !important;
    border: 1px solid rgba(13, 110, 253, 0.6) !important;
    box-shadow: 0 0 0 0.25rem rgba(13, 110, 253, 0.15) !important;
}

/* 暗色主题下的下拉菜单透明度调整 - 移除backdrop-filter */
[data-bs-theme="dark"] body.custom-background-enabled .dropdown-menu {
    background-color: rgba(30, 30, 30, var(--page-opacity)) !important;
    /* 移除backdrop-filter避免创建层叠上下文，确保z-index正常工作 */
    /* backdrop-filter: blur(5px); */
    /* -webkit-backdrop-filter: blur(5px); */
}

/* 暗色主题下的滑块完全透明化 - 完整重置 */
[data-bs-theme="dark"] body.custom-background-enabled .form-range {
    -webkit-appearance: none !important;
    appearance: none !important;
    background: transparent !important;
    outline: none !important;
}

/* WebKit浏览器 (Chrome, Safari) - 暗色主题 */
[data-bs-theme="dark"] body.custom-background-enabled .form-range::-webkit-slider-track {
    -webkit-appearance: none !important;
    appearance: none !important;
    background: transparent !important;
    border: 1px solid rgba(255, 255, 255, 0.2) !important;
    height: 6px !important;
    border-radius: 3px !important;
    box-shadow: none !important;
    outline: none !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .form-range::-webkit-slider-runnable-track {
    -webkit-appearance: none !important;
    background: transparent !important;
    border: 1px solid rgba(255, 255, 255, 0.2) !important;
    height: 6px !important;
    border-radius: 3px !important;
    box-shadow: none !important;
}

/* Firefox - 暗色主题 */
[data-bs-theme="dark"] body.custom-background-enabled .form-range::-moz-range-track {
    background: transparent !important;
    border: 1px solid rgba(255, 255, 255, 0.2) !important;
    height: 6px !important;
    border-radius: 3px !important;
    box-shadow: none !important;
    outline: none !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .form-range::-moz-range-progress {
    background: transparent !important;
    height: 6px !important;
    border-radius: 3px !important;
}

/* 滑块按钮 - 暗色主题 - 垂直居中对齐 */
[data-bs-theme="dark"] body.custom-background-enabled .form-range::-webkit-slider-thumb {
    -webkit-appearance: none !important;
    appearance: none !important;
    background-color: rgba(13, 110, 253, 0.9) !important;
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
    width: 20px !important;
    height: 20px !important;
    border-radius: 50% !important;
    cursor: pointer !important;
    margin-top: -7px !important;
    box-sizing: border-box !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .form-range::-moz-range-thumb {
    background-color: rgba(13, 110, 253, 0.9) !important;
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
    width: 20px !important;
    height: 20px !important;
    border-radius: 50% !important;
    cursor: pointer !important;
    box-shadow: none !important;
    margin-top: -8px !important;
    box-sizing: border-box !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .mobile-server-card,
[data-bs-theme="dark"] body.custom-background-enabled .mobile-site-card {
    background-color: rgba(33, 37, 41, var(--page-opacity)) !important;
    border-color: rgba(73, 80, 87, var(--page-opacity));
}

[data-bs-theme="dark"] body.custom-background-enabled .mobile-card-header {
    background-color: rgba(255, 255, 255, calc(0.05 * var(--page-opacity))) !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .table-striped > tbody > tr:nth-of-type(odd) > * {
    background-color: rgba(255, 255, 255, 0.03) !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .table-hover > tbody > tr:hover > * {
    background-color: rgba(255, 255, 255, 0.05) !important;
}





/* 警告框透明度调整 */
body.custom-background-enabled #serverAlert,
body.custom-background-enabled #siteAlert,
body.custom-background-enabled #telegramSettingsAlert,
body.custom-background-enabled #ntfySettingsAlert,
body.custom-background-enabled #backgroundSettingsAlert {
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    box-shadow: 0 0.5rem 1rem rgba(0, 0, 0, 0.3);
}

/* ==================== 文字描边渲染系统 ==================== */

/* 文字加粗系统 - 精简版 */
p, div, span:not(.badge), td, th, .btn, button, a:not(.navbar-brand),
.form-control, .form-select, .form-check-label, input, textarea,
.card-header, .card-title, .card-body, .modal-content, .modal-title, .dropdown-menu,
.progress span, .alert, .breadcrumb, .list-group-item {
    font-weight: 500;
}

/* 统一Toast弹窗系统 */
.toast-container {
    position: fixed;
    top: 15%;
    left: 50%;
    transform: translateX(-50%);
    z-index: 10000; /* 确保在所有元素之上，包括模态框 */
    pointer-events: none;
    display: flex;
    flex-direction: column;
    align-items: center;
}

.unified-toast {
    pointer-events: auto;
    min-width: 120px;
    max-width: 90vw;
    padding: 16px 50px 16px 24px;
    margin-bottom: 12px;
    border-radius: 12px;
    backdrop-filter: blur(16px);
    border: 1px solid rgba(255, 255, 255, 0.2);
    font-weight: 500;
    font-size: 15px;
    position: relative;
    display: inline-flex;
    align-items: center;
    animation: toastIn 0.3s ease;
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.15);
}

.unified-toast.hiding {
    animation: toastOut 0.3s ease;
    opacity: 0;
}

.unified-toast.success {
    background: linear-gradient(135deg,
        rgba(34, 197, 94, calc(0.7 * var(--page-opacity, 0.8))),
        rgba(22, 163, 74, calc(0.7 * var(--page-opacity, 0.8))));
    color: white;
    border-color: rgba(34, 197, 94, calc(0.4 * var(--page-opacity, 0.8)));
}

.unified-toast.danger {
    background: linear-gradient(135deg,
        rgba(239, 68, 68, calc(0.7 * var(--page-opacity, 0.8))),
        rgba(220, 38, 38, calc(0.7 * var(--page-opacity, 0.8))));
    color: white;
    border-color: rgba(239, 68, 68, calc(0.4 * var(--page-opacity, 0.8)));
}

.unified-toast.warning {
    background: linear-gradient(135deg,
        rgba(245, 158, 11, calc(0.7 * var(--page-opacity, 0.8))),
        rgba(217, 119, 6, calc(0.7 * var(--page-opacity, 0.8))));
    color: white;
    border-color: rgba(245, 158, 11, calc(0.4 * var(--page-opacity, 0.8)));
}

.unified-toast.info {
    background: linear-gradient(135deg,
        rgba(59, 130, 246, calc(0.7 * var(--page-opacity, 0.8))),
        rgba(37, 99, 235, calc(0.7 * var(--page-opacity, 0.8))));
    color: white;
    border-color: rgba(59, 130, 246, calc(0.4 * var(--page-opacity, 0.8)));
}

.toast-icon {
    margin-right: 8px;
    font-size: 16px;
    flex-shrink: 0;
}

.toast-content {
    flex: 1;
    line-height: 1.4;
}

.toast-close {
    position: absolute;
    top: 50%;
    right: 12px;
    transform: translateY(-50%);
    background: none;
    border: none;
    color: rgba(255, 255, 255, 0.8);
    font-size: 16px;
    cursor: pointer;
    padding: 6px;
    border-radius: 50%;
    width: 28px;
    height: 28px;
}

.toast-close:hover {
    background: rgba(255, 255, 255, 0.2);
}

.toast-progress {
    position: absolute;
    bottom: 0;
    left: 0;
    height: 3px;
    background: rgba(255, 255, 255, 0.3);
    border-radius: 0 0 12px 12px;
    animation: progressBar 5s linear;
}

@keyframes toastIn {
    from { opacity: 0; transform: translateY(-20px); }
    to { opacity: 1; transform: translateY(0); }
}

@keyframes toastOut {
    from { opacity: 1; }
    to { opacity: 0; }
}

@keyframes progressBar {
    from { width: 100%; }
    to { width: 0%; }
}

[data-bs-theme="dark"] .unified-toast {
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
    border-color: rgba(255, 255, 255, 0.1);
}

/* 自定义导航栏高度 */
.navbar {
    --bs-navbar-padding-y: 0.375rem;
    min-height: 50px;
    height: 50px;
}

.navbar-brand {
    padding-top: 0.3125rem;
    padding-bottom: 0.3125rem;
    line-height: 1.25;
}


`;
}

function getMainJs() {
  return `// main.js - 首页面的JavaScript逻辑

// Global variables
let vpsUpdateInterval = null;
let siteUpdateInterval = null;
let serverDataCache = {}; // Cache server data to avoid re-fetching for details
let vpsStatusCache = {}; // 用于跟踪VPS状态变化
const llmProviderExpansionState = {};
const DEFAULT_VPS_REFRESH_INTERVAL_MS = 60000; // Default to 60 seconds for VPS data if backend setting fails
const DEFAULT_SITE_REFRESH_INTERVAL_MS = 60000; // Default to 60 seconds for Site data

// ==================== 统一API请求工具 ====================

// 获取认证头
function getAuthHeaders() {
    const token = localStorage.getItem('auth_token');
    const headers = { 'Content-Type': 'application/json' };
    if (token) {
        headers['Authorization'] = 'Bearer ' + token;
    }
    return headers;
}

// ==================== VPS状态变化检测 ====================

// 检测VPS状态变化并发送通知
async function checkVpsStatusChanges(allStatuses) {
    for (const data of allStatuses) {
        const serverId = data.server.id;
        const serverName = data.server.name;
        const currentStatus = determineVpsStatus(data);
        const previousStatus = vpsStatusCache[serverId];

        // 首次加载或状态变化时检测
        if (previousStatus === undefined || previousStatus !== currentStatus) {
                        if (currentStatus === 'offline') {
                await notifyVpsOffline(serverId, serverName);
            } else if (currentStatus === 'online' && previousStatus === 'offline') {
                await notifyVpsRecovery(serverId, serverName);
            }
        }

        vpsStatusCache[serverId] = currentStatus;
    }
}

// 判断VPS状态
function determineVpsStatus(data) {
    if (data.error) return 'error';
    if (!data.metrics) return 'unknown';

    const now = new Date();
    const lastReportTime = new Date(data.metrics.timestamp * 1000);
    const diffMinutes = (now - lastReportTime) / (1000 * 60);

    return diffMinutes <= 5 ? 'online' : 'offline';
}

// 发送VPS离线通知
async function notifyVpsOffline(serverId, serverName) {
    try {
        // 使用完整URL
        const baseUrl = window.location.origin;
        await fetch(baseUrl + '/api/notify/offline', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ serverId, serverName })
        });
            } catch (error) {
            }
}

// 发送VPS恢复通知
async function notifyVpsRecovery(serverId, serverName) {
    try {
        // 使用完整URL
        const baseUrl = window.location.origin;
        await fetch(baseUrl + '/api/notify/recovery', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ serverId, serverName })
        });
            } catch (error) {
            }
}

// 统一API请求函数（用于需要认证的请求）
async function apiRequest(url, options = {}) {
    const defaultOptions = {
        headers: getAuthHeaders(),
        ...options
    };

    try {
        const response = await fetch(url, defaultOptions);

        // 处理认证失败
        if (response.status === 401) {
            localStorage.removeItem('auth_token');
            if (window.location.pathname !== '/login.html') {
                window.location.href = 'login.html';
            }
            throw new Error('认证失败，请重新登录');
        }

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.message || \`请求失败 (\${response.status})\`);
        }

        return await response.json();
    } catch (error) {
                throw error;
    }
}

// 公开API请求函数（用于不需要认证的请求）
async function publicApiRequest(url, options = {}) {
    const defaultOptions = {
        headers: getAuthHeaders(), // 仍然发送token（如果有），但不强制要求
        ...options
    };

    try {
        const response = await fetch(url, defaultOptions);

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.message || \`请求失败 (\${response.status})\`);
        }

        return await response.json();
    } catch (error) {
                throw error;
    }
}

// 显示错误消息
function showError(message, containerId = null) {
    console.error('错误:', message);
    if (containerId) {
        const container = document.getElementById(containerId);
        if (container) {
            container.innerHTML = \`<div class="alert alert-danger">\${message}</div>\`;
        }
    }
}

// 显示成功消息
function showSuccess(message, containerId = null) {
        if (containerId) {
        const container = document.getElementById(containerId);
        if (container) {
            container.innerHTML = \`<div class="alert alert-success">\${message}</div>\`;
        }
    }
}

function escapeHtml(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function getProviderFallbackName(apiUrl) {
    if (!apiUrl) return '未命名 Provider';
    try {
        const parsed = new URL(apiUrl);
        return parsed.hostname.replace(/^www\./, '') || apiUrl;
    } catch (error) {
        return apiUrl;
    }
}

function groupLlmEndpointsByProvider(endpoints) {
    const groups = [];
    const groupMap = new Map();

    endpoints.forEach((endpoint) => {
        const key = endpoint.api_url || endpoint.id;
        let group = groupMap.get(key);
        if (!group) {
            group = {
                key,
                api_url: endpoint.api_url,
                providerName: (endpoint.name || '').trim(),
                endpoints: []
            };
            groupMap.set(key, group);
            groups.push(group);
        } else if (endpoint.name && endpoint.name.trim()) {
            group.providerName = endpoint.name.trim();
        }

        group.endpoints.push(endpoint);
    });

    groups.forEach((group) => {
        if (!group.providerName) {
            group.providerName = getProviderFallbackName(group.api_url);
        }
        if (typeof llmProviderExpansionState[group.key] === 'undefined') {
            llmProviderExpansionState[group.key] = true;
        }
    });

    return groups;
}

function getProviderStatusInfo(group) {
    const statuses = group.endpoints.map(ep => ep.last_status);
    if (statuses.some(status => ['DOWN', 'ERROR'].includes(status))) return getSiteStatusBadge('DOWN');
    if (statuses.includes('TIMEOUT')) return getSiteStatusBadge('TIMEOUT');
    if (statuses.includes('HIGH_LATENCY')) return getSiteStatusBadge('HIGH_LATENCY');
    if (statuses.includes('UP')) return getSiteStatusBadge('UP');
    return getSiteStatusBadge('PENDING');
}

function getProviderHeaderMeta(group) {
    const hostName = getProviderFallbackName(group.api_url);
    const customName = (group.providerName || '').trim();
    const childModels = new Set(group.endpoints.map(ep => (ep.model || '').trim()).filter(Boolean));
    const conflictsWithModel = customName && childModels.has(customName);

    return {
        title: customName && !conflictsWithModel ? customName : hostName,
        subtitle: customName && customName !== hostName
            ? (conflictsWithModel ? '别名: ' + customName : hostName)
            : (group.api_url || '-'),
        endpointText: group.api_url || '-'
    };
}

// Function to fetch VPS refresh interval and start periodic VPS data updates
async function initializeVpsDataUpdates() {
        let vpsRefreshIntervalMs = DEFAULT_VPS_REFRESH_INTERVAL_MS;

    try {
                const data = await publicApiRequest('/api/admin/settings/vps-report-interval');
                if (data && typeof data.interval === 'number' && data.interval > 0) {
            vpsRefreshIntervalMs = data.interval * 1000; // Convert seconds to milliseconds
                    } else {
            // 使用默认值
        }
    } catch (error) {
            }

    // Clear existing interval if any
    if (vpsUpdateInterval) {
                clearInterval(vpsUpdateInterval);
    }

    // VPS数据跟随后台设置频率刷新
        vpsUpdateInterval = setInterval(() => {
                loadAllServerStatuses();
    }, vpsRefreshIntervalMs);

    }

// 优化：网站状态每小时刷新一次
function initializeSiteDataUpdates() {
    const hourlyRefreshInterval = 60 * 60 * 1000; // 1小时
        // 清除任何现有的自动刷新间隔
    if (siteUpdateInterval) {
        clearInterval(siteUpdateInterval);
    }

    // 设置每小时刷新一次
    siteUpdateInterval = setInterval(() => {
                loadAllSiteStatuses();
    }, hourlyRefreshInterval);

    }

// LLM端点状态每小时刷新一次
let llmUpdateInterval = null;
function initializeLlmDataUpdates() {
    const hourlyRefreshInterval = 60 * 60 * 1000;
    if (llmUpdateInterval) {
        clearInterval(llmUpdateInterval);
    }
    llmUpdateInterval = setInterval(() => {
        loadAllLlmEndpointStatuses();
    }, hourlyRefreshInterval);
}

// 移除手动刷新按钮相关代码，改为自动刷新

// Execute after the page loads (only for main page)
document.addEventListener('DOMContentLoaded', function() {
        // Check if we're on the main page by looking for the server table
    const serverTableBody = document.getElementById('serverTableBody');
    if (!serverTableBody) {
        // Not on the main page, only initialize theme
                initializeTheme();
        return;
    }

        // Initialize theme
    initializeTheme();

    // Load initial data
    loadAllServerStatuses();
    loadAllSiteStatuses();
    loadAllLlmEndpointStatuses();

    // Initialize periodic updates separately
        initializeVpsDataUpdates();
        initializeSiteDataUpdates();
        initializeLlmDataUpdates();

    // Add click event listener to the table body for row expansion
    serverTableBody.addEventListener('click', handleRowClick);

    // Check login status and update admin link
    updateAdminLink();
});

// --- Theme Management ---
const THEME_KEY = 'vps-monitor-theme';
const LIGHT_THEME = 'light';
const DARK_THEME = 'dark';

function initializeTheme() {
    const themeToggler = document.getElementById('themeToggler');
    if (!themeToggler) return;

    const storedTheme = localStorage.getItem(THEME_KEY) || LIGHT_THEME;
    applyTheme(storedTheme);

    themeToggler.addEventListener('click', () => {
        const currentTheme = document.documentElement.getAttribute('data-bs-theme');
        const newTheme = currentTheme === DARK_THEME ? LIGHT_THEME : DARK_THEME;
        applyTheme(newTheme);
        localStorage.setItem(THEME_KEY, newTheme);
    });
}

function applyTheme(theme) {
    document.documentElement.setAttribute('data-bs-theme', theme);
    const themeTogglerIcon = document.querySelector('#themeToggler i');
    if (themeTogglerIcon) {
        if (theme === DARK_THEME) {
            themeTogglerIcon.classList.remove('bi-moon-stars-fill');
            themeTogglerIcon.classList.add('bi-sun-fill');
        } else {
            themeTogglerIcon.classList.remove('bi-sun-fill');
            themeTogglerIcon.classList.add('bi-moon-stars-fill');
        }
    }
}
// --- End Theme Management ---

// Check login status and update the admin link in the navbar
async function updateAdminLink() {
    const adminLink = document.getElementById('adminAuthLink');
    if (!adminLink) return; // Exit if link not found

    try {
        const token = localStorage.getItem('auth_token');
        if (!token) {
            // Not logged in (no token)
            adminLink.textContent = '管理员登录';
            adminLink.href = '/login.html';
            return;
        }

        const data = await publicApiRequest('/api/auth/status');
        if (data.authenticated) {
            // Logged in
            adminLink.textContent = '管理后台';
            adminLink.href = '/admin.html';
        } else {
            // Invalid token or not authenticated
            adminLink.textContent = '管理员登录';
            adminLink.href = '/login.html';
            localStorage.removeItem('auth_token'); // Clean up invalid token
        }
    } catch (error) {
                // Network error, assume not logged in
        adminLink.textContent = '管理员登录';
        adminLink.href = '/login.html';
    }
}


// Handle click on a server row
function handleRowClick(event) {
    const clickedRow = event.target.closest('tr.server-row');
    if (!clickedRow) return; // Not a server row

    const serverId = clickedRow.getAttribute('data-server-id');
    const detailsRow = clickedRow.nextElementSibling; // The details row is the next sibling

    if (detailsRow && detailsRow.classList.contains('server-details-row')) {
        // Toggle visibility
        detailsRow.classList.toggle('d-none');

        // If showing, populate with detailed data
        if (!detailsRow.classList.contains('d-none')) {
            populateDetailsRow(serverId, detailsRow);
        }
    }
}

// Populate the detailed row with data
function populateDetailsRow(serverId, detailsRow) {
    const serverData = serverDataCache[serverId];
    const detailsContentDiv = detailsRow.querySelector('.server-details-content');

    if (!serverData || !serverData.metrics || !detailsContentDiv) {
        detailsContentDiv.innerHTML = '<p class="text-muted">无详细数据</p>';
        return;
    }

    const metrics = serverData.metrics;

    let detailsHtml = '';

    // CPU Details
    if (metrics.cpu && metrics.cpu.load_avg) {
        detailsHtml += \`
            <div class="detail-item">
                <strong>CPU负载 (1m, 5m, 15m):</strong> \${metrics.cpu.load_avg.join(', ')}
            </div>
        \`;
    }

    // Memory Details
    if (metrics.memory) {
        detailsHtml += \`
            <div class="detail-item">
                <strong>内存:</strong>
                总计: \${formatDataSize(metrics.memory.total * 1024)}<br>
                已用: \${formatDataSize(metrics.memory.used * 1024)}<br>
                空闲: \${formatDataSize(metrics.memory.free * 1024)}
            </div>
        \`;
    }

    // Disk Details
    if (metrics.disk) {
         detailsHtml += \`
            <div class="detail-item">
                <strong>硬盘 (/):</strong>
                总计: \${typeof metrics.disk.total === 'number' ? metrics.disk.total.toFixed(2) : '-'} GB<br>
                已用: \${typeof metrics.disk.used === 'number' ? metrics.disk.used.toFixed(2) : '-'} GB<br>
                空闲: \${typeof metrics.disk.free === 'number' ? metrics.disk.free.toFixed(2) : '-'} GB
            </div>
        \`;
    }

    // Network Totals
    if (metrics.network) {
        detailsHtml += \`
            <div class="detail-item">
                <strong>总流量:</strong>
                上传: \${formatDataSize(metrics.network.total_upload)}<br>
                下载: \${formatDataSize(metrics.network.total_download)}
            </div>
        \`;
    }

    detailsContentDiv.innerHTML = detailsHtml || '<p class="text-muted">无详细数据</p>';
}


// Load all server statuses
async function loadAllServerStatuses() {
        try {
        // 使用批量API一次性获取所有VPS状态
        let batchData;
        try {
            batchData = await publicApiRequest('/api/status/batch');
        } catch (error) {
            // 如果批量API失败，可能是数据库未初始化，尝试初始化
                        await publicApiRequest('/api/init-db');
            batchData = await publicApiRequest('/api/status/batch');
        }

        const allStatuses = batchData.servers || [];
                const noServersAlert = document.getElementById('noServers');
        const serverTableBody = document.getElementById('serverTableBody');

        if (allStatuses.length === 0) {
            noServersAlert.classList.remove('d-none');
            serverTableBody.innerHTML = '<tr><td colspan="11" class="text-center">No server data available. Please log in to the admin panel to add servers.</td></tr>';
            // Remove any existing detail rows if the server list becomes empty
            removeAllDetailRows();
            // 同时更新移动端卡片容器
            renderMobileServerCards([]);
            return;
        } else {
            noServersAlert.classList.add('d-none');
        }

        // Update the serverDataCache with the latest data
        allStatuses.forEach(data => {
             serverDataCache[data.server.id] = data;
        });

        // 检测VPS状态变化并发送通知
        await checkVpsStatusChanges(allStatuses);

        // 3. Render the table using DOM manipulation
        renderServerTable(allStatuses);

    } catch (error) {
                const serverTableBody = document.getElementById('serverTableBody');
        serverTableBody.innerHTML = '<tr><td colspan="11" class="text-center text-danger">Failed to load server data. Please refresh the page.</td></tr>';
        removeAllDetailRows();
        // 同时更新移动端卡片容器显示错误状态
        showToast('danger', '加载服务器数据失败，请刷新页面重试');
    }
}

// Remove all existing server detail rows
function removeAllDetailRows() {
    document.querySelectorAll('.server-details-row').forEach(row => row.remove());
}


// Generate progress bar HTML
function getProgressBarHtml(percentage) {
    if (typeof percentage !== 'number' || isNaN(percentage)) return '-';
    const percent = Math.max(0, Math.min(100, percentage)); // Ensure percentage is between 0 and 100
    let bgColorClass = 'bg-light-green'; // Use custom light green for < 50%

    if (percent >= 80) {
        bgColorClass = 'bg-danger'; // Red for >= 80%
    } else if (percent >= 50) {
        bgColorClass = 'bg-warning'; // Yellow for 50% - 79%
    }

    // Use relative positioning on the container and absolute for the text, centered over the whole bar
    return \`
        <div class="progress" style="height: 25px; font-size: 0.8em; position: relative; background-color: #e9ecef;">
            <div class="progress-bar \${bgColorClass}" role="progressbar" style="width: \${percent}%;" aria-valuenow="\${percent}" aria-valuemin="0" aria-valuemax="100"></div>
            <span style="position: absolute; width: 100%; text-align: center; line-height: 25px; font-weight: bold;">
                \${percent.toFixed(1)}%
            </span>
        </div>
    \`;
}


// 移动端辅助函数
function getServerStatusBadge(status) {
    if (status === 'online') {
        return { class: 'bg-success', text: '在线' };
    } else if (status === 'offline') {
        return { class: 'bg-danger', text: '离线' };
    } else if (status === 'error') {
        return { class: 'bg-warning text-dark', text: '错误' };
    } else {
        return { class: 'bg-secondary', text: '未知' };
    }
}


// 移动端服务器卡片渲染函数
function renderMobileServerCards(allStatuses) {
    const mobileContainer = document.getElementById('mobileServerContainer');
    if (!mobileContainer) return;

    mobileContainer.innerHTML = '';

    if (!allStatuses || allStatuses.length === 0) {
        mobileContainer.innerHTML = \`
            <div class="text-center p-4">
                <i class="bi bi-server text-muted" style="font-size: 3rem;"></i>
                <div class="mt-3 text-muted">
                    <h6>暂无服务器数据</h6>
                    <small>请登录管理后台添加服务器</small>
                </div>
            </div>
        \`;
        return;
    }

    allStatuses.forEach(data => {
        const serverId = data.server.id;
        const serverName = data.server.name;
        const metrics = data.metrics;
        const hasError = data.error;

        const card = document.createElement('div');
        card.className = 'mobile-server-card';
        card.setAttribute('data-server-id', serverId);

        // 确定服务器状态
        let status = 'unknown';
        let lastUpdate = '从未';

        if (hasError) {
            status = 'error';
        } else if (metrics) {
            const now = new Date();
            const lastReportTime = new Date(metrics.timestamp * 1000);
            const diffMinutes = (now - lastReportTime) / (1000 * 60);

            if (diffMinutes <= 5) {
                status = 'online';
            } else {
                status = 'offline';
            }
            lastUpdate = lastReportTime.toLocaleString();
        }

        const statusInfo = getServerStatusBadge(status);

        // 卡片头部
        const cardHeader = document.createElement('div');
        cardHeader.className = 'mobile-card-header';
        cardHeader.innerHTML = \`
            <div style="flex: 1;"></div>
            <h6 class="mobile-card-title text-center" style="flex: 1;">\${serverName || '未命名服务器'}</h6>
            <div style="flex: 1; display: flex; justify-content: flex-end;">
                <span class="badge \${statusInfo.class}">\${statusInfo.text}</span>
            </div>
        \`;

        // 卡片主体 - 显示所有信息
        const cardBody = document.createElement('div');
        cardBody.className = 'mobile-card-body';

        // 获取所有数据
        const cpuValue = metrics && metrics.cpu && typeof metrics.cpu.usage_percent === 'number' ? \`\${metrics.cpu.usage_percent.toFixed(1)}%\` : '-';
        const memoryValue = metrics && metrics.memory && typeof metrics.memory.usage_percent === 'number' ? \`\${metrics.memory.usage_percent.toFixed(1)}%\` : '-';
        const diskValue = metrics && metrics.disk && typeof metrics.disk.usage_percent === 'number' ? \`\${metrics.disk.usage_percent.toFixed(1)}%\` : '-';
        const uptimeValue = metrics && metrics.uptime ? formatUptime(metrics.uptime) : '-';
        const uploadSpeed = metrics && metrics.network ? formatNetworkSpeed(metrics.network.upload_speed) : '-';
        const downloadSpeed = metrics && metrics.network ? formatNetworkSpeed(metrics.network.download_speed) : '-';
        const totalUpload = metrics && metrics.network ? formatDataSize(metrics.network.total_upload) : '-';
        const totalDownload = metrics && metrics.network ? formatDataSize(metrics.network.total_download) : '-';

        // 上传速度 | 下载速度
        const speedRow = document.createElement('div');
        speedRow.className = 'mobile-card-two-columns';
        speedRow.innerHTML = \`
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">上传速度</span>
                <span class="mobile-card-value">\${uploadSpeed}</span>
            </div>
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">下载速度</span>
                <span class="mobile-card-value">\${downloadSpeed}</span>
            </div>
        \`;
        cardBody.appendChild(speedRow);

        // CPU | 内存
        const cpuMemoryRow = document.createElement('div');
        cpuMemoryRow.className = 'mobile-card-two-columns';
        cpuMemoryRow.innerHTML = \`
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">CPU</span>
                <span class="mobile-card-value">\${cpuValue}</span>
            </div>
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">内存</span>
                <span class="mobile-card-value">\${memoryValue}</span>
            </div>
        \`;
        cardBody.appendChild(cpuMemoryRow);

        // 硬盘 | 运行时长
        const diskUptimeRow = document.createElement('div');
        diskUptimeRow.className = 'mobile-card-two-columns';
        diskUptimeRow.innerHTML = \`
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">硬盘</span>
                <span class="mobile-card-value">\${diskValue}</span>
            </div>
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">运行时长</span>
                <span class="mobile-card-value">\${uptimeValue}</span>
            </div>
        \`;
        cardBody.appendChild(diskUptimeRow);

        // 总上传 | 总下载
        const totalRow = document.createElement('div');
        totalRow.className = 'mobile-card-two-columns';
        totalRow.innerHTML = \`
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">总上传</span>
                <span class="mobile-card-value">\${totalUpload}</span>
            </div>
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">总下载</span>
                <span class="mobile-card-value">\${totalDownload}</span>
            </div>
        \`;
        cardBody.appendChild(totalRow);

        // 最后更新 - 单行
        const lastUpdateRow = document.createElement('div');
        lastUpdateRow.className = 'mobile-card-row';
        lastUpdateRow.innerHTML = \`
            <span class="mobile-card-label">最后更新: \${lastUpdate}</span>
        \`;
        cardBody.appendChild(lastUpdateRow);

        // 组装卡片
        card.appendChild(cardHeader);
        card.appendChild(cardBody);

        mobileContainer.appendChild(card);
    });
}

// 移动端网站卡片渲染函数
function renderMobileSiteCards(sites) {
    const mobileContainer = document.getElementById('mobileSiteContainer');
    if (!mobileContainer) return;

    mobileContainer.innerHTML = '';

    if (!sites || sites.length === 0) {
        mobileContainer.innerHTML = \`
            <div class="text-center p-4">
                <i class="bi bi-globe text-muted" style="font-size: 3rem;"></i>
                <div class="mt-3 text-muted">
                    <h6>暂无监控网站数据</h6>
                    <small>请登录管理后台添加监控网站</small>
                </div>
            </div>
        \`;
        return;
    }

    sites.forEach(site => {
        const card = document.createElement('div');
        card.className = 'mobile-site-card';

        const statusInfo = getSiteStatusBadge(site.last_status);
        const lastCheckTime = site.last_checked ? new Date(site.last_checked * 1000).toLocaleString() : '从未';
        const responseTime = site.last_response_time_ms !== null ? \`\${site.last_response_time_ms} ms\` : '-';

        // 卡片头部
        const cardHeader = document.createElement('div');
        cardHeader.className = 'mobile-card-header';
        const isSiteUp = (site.last_status === 'UP' || site.last_status === 'HIGH_LATENCY');
        const titleHtml = isSiteUp && site.name
            ? \`<a href="\${site.url}" target="_blank" rel="noopener noreferrer" class="site-name-link">\${site.name}</a>\`
            : (site.name || '未命名网站');
        cardHeader.innerHTML = \`
            <div style="flex: 1;"></div>
            <h6 class="mobile-card-title text-center" style="flex: 1;">\${titleHtml}</h6>
            <div style="flex: 1; display: flex; justify-content: flex-end;">
                <span class="badge \${statusInfo.class}">\${statusInfo.text}</span>
            </div>
        \`;

        // 卡片主体
        const cardBody = document.createElement('div');
        cardBody.className = 'mobile-card-body';

        // 网站信息 - 两列布局
        const statusCode = site.last_status_code || '-';

        // 状态码 | 响应时间
        const statusResponseRow = document.createElement('div');
        statusResponseRow.className = 'mobile-card-two-columns';
        statusResponseRow.innerHTML = \`
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">状态码</span>
                <span class="mobile-card-value">\${statusCode}</span>
            </div>
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">响应时间</span>
                <span class="mobile-card-value">\${responseTime}</span>
            </div>
        \`;
        cardBody.appendChild(statusResponseRow);

        // 最后检查 - 单行
        const lastCheckRow = document.createElement('div');
        lastCheckRow.className = 'mobile-card-row';
        lastCheckRow.innerHTML = \`
            <span class="mobile-card-label">最后检查: \${lastCheckTime}</span>
        \`;
        cardBody.appendChild(lastCheckRow);

        // 24小时历史记录 - 始终显示，即使没有数据
        const historyContainer = document.createElement('div');
        historyContainer.className = 'mobile-history-container';
        historyContainer.innerHTML = \`
            <div class="mobile-history-label">24小时记录</div>
            <div class="history-bar-container"></div>
        \`;
        cardBody.appendChild(historyContainer);

        // 使用统一的历史记录渲染函数
        const historyBarContainer = historyContainer.querySelector('.history-bar-container');
        renderSiteHistoryBar(historyBarContainer, site.history || []);

        // 组装卡片
        card.appendChild(cardHeader);
        card.appendChild(cardBody);

        mobileContainer.appendChild(card);
    });
}





// Render the server table using DOM manipulation
function renderServerTable(allStatuses) {
    const tableBody = document.getElementById('serverTableBody');
    const detailsTemplate = document.getElementById('serverDetailsTemplate');

    // 1. Store IDs of currently expanded servers
    const expandedServerIds = new Set();
    // Iterate over main server rows to find their expanded detail rows
    tableBody.querySelectorAll('tr.server-row').forEach(mainRow => {
        const detailRow = mainRow.nextElementSibling;
        if (detailRow && detailRow.classList.contains('server-details-row') && !detailRow.classList.contains('d-none')) {
            const serverId = mainRow.getAttribute('data-server-id');
            if (serverId) {
                expandedServerIds.add(serverId);
            }
        }
    });

    tableBody.innerHTML = ''; // Clear existing rows

    allStatuses.forEach(data => {
        const serverId = data.server.id;
        const serverName = data.server.name;
        const metrics = data.metrics;
        const hasError = data.error;

        let statusBadge = '<span class="badge bg-secondary">未知</span>';
        let cpuHtml = '-';
        let memoryHtml = '-';
        let diskHtml = '-';
        let uploadSpeed = '-';
        let downloadSpeed = '-';
        let totalUpload = '-';
        let totalDownload = '-';
        let uptime = '-';
        let lastUpdate = '-';

        if (hasError) {
            statusBadge = '<span class="badge bg-warning text-dark">错误</span>';
        } else if (metrics) {
            const now = new Date();
            const lastReportTime = new Date(metrics.timestamp * 1000);
            const diffMinutes = (now - lastReportTime) / (1000 * 60);

            if (diffMinutes <= 5) { // Considered online within 5 minutes
                statusBadge = '<span class="badge bg-success">在线</span>';
            } else {
                statusBadge = '<span class="badge bg-danger">离线</span>';
            }

            cpuHtml = getProgressBarHtml(metrics.cpu.usage_percent);
            memoryHtml = getProgressBarHtml(metrics.memory.usage_percent);
            diskHtml = getProgressBarHtml(metrics.disk.usage_percent);
            uploadSpeed = formatNetworkSpeed(metrics.network.upload_speed);
            downloadSpeed = formatNetworkSpeed(metrics.network.download_speed);
            totalUpload = formatDataSize(metrics.network.total_upload);
            totalDownload = formatDataSize(metrics.network.total_download);
            uptime = metrics.uptime ? formatUptime(metrics.uptime) : '-';
            lastUpdate = lastReportTime.toLocaleString();
        }

        // Create the main row
        const mainRow = document.createElement('tr');
        mainRow.classList.add('server-row');
        mainRow.setAttribute('data-server-id', serverId);
        mainRow.innerHTML = \`
            <td>\${serverName}</td>
            <td>\${statusBadge}</td>
            <td>\${cpuHtml}</td>
            <td>\${memoryHtml}</td>
            <td>\${diskHtml}</td>
            <td><span style="color: #000;">\${uploadSpeed}</span></td>
            <td><span style="color: #000;">\${downloadSpeed}</span></td>
            <td><span style="color: #000;">\${totalUpload}</span></td>
            <td><span style="color: #000;">\${totalDownload}</span></td>
            <td><span style="color: #000;">\${uptime}</span></td>
            <td><span style="color: #000;">\${lastUpdate}</span></td>
        \`;

        // Clone the details row template
        const detailsRowElement = detailsTemplate.content.cloneNode(true).querySelector('tr');
        // The template has d-none by default. We will remove it if needed.
        // Set a unique attribute for easier selection if needed, though direct reference is used here.
        // detailsRowElement.setAttribute('data-detail-for', serverId);

        tableBody.appendChild(mainRow);
        tableBody.appendChild(detailsRowElement);

        // 2. If this server was previously expanded, re-expand it and populate its details
        if (expandedServerIds.has(serverId)) {
            detailsRowElement.classList.remove('d-none');
            populateDetailsRow(serverId, detailsRowElement); // Populate content
        }
    });

    // 3. 同时渲染移动端卡片
    renderMobileServerCards(allStatuses);
}


// Format network speed
function formatNetworkSpeed(bytesPerSecond) {
    if (typeof bytesPerSecond !== 'number' || isNaN(bytesPerSecond)) return '-';
    if (bytesPerSecond < 1024) {
        return \`\${bytesPerSecond.toFixed(1)} B/s\`;
    } else if (bytesPerSecond < 1024 * 1024) {
        return \`\${(bytesPerSecond / 1024).toFixed(1)} KB/s\`;
    } else if (bytesPerSecond < 1024 * 1024 * 1024) {
        return \`\${(bytesPerSecond / (1024 * 1024)).toFixed(1)} MB/s\`;
    } else {
        return \`\${(bytesPerSecond / (1024 * 1024 * 1024)).toFixed(1)} GB/s\`;
    }
}

// Format data size
function formatDataSize(bytes) {
    if (typeof bytes !== 'number' || isNaN(bytes)) return '-';
    if (bytes < 1024) {
        return \`\${bytes.toFixed(1)} B\`;
    } else if (bytes < 1024 * 1024) {
        return \`\${(bytes / 1024).toFixed(1)} KB\`;
    } else if (bytes < 1024 * 1024 * 1024) {
        return \`\${(bytes / (1024 * 1024)).toFixed(1)} MB\`;
    } else if (bytes < 1024 * 1024 * 1024 * 1024) {
        return \`\${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB\`;
    } else {
        return \`\${(bytes / (1024 * 1024 * 1024 * 1024)).toFixed(1)} TB\`;
    }
}

// Format uptime from seconds to a human-readable string
function formatUptime(totalSeconds) {
    if (typeof totalSeconds !== 'number' || isNaN(totalSeconds) || totalSeconds < 0) {
        return '-';
    }

    const days = Math.floor(totalSeconds / (3600 * 24));
    totalSeconds %= (3600 * 24);
    const hours = Math.floor(totalSeconds / 3600);
    totalSeconds %= 3600;
    const minutes = Math.floor(totalSeconds / 60);

    let uptimeString = '';
    if (days > 0) {
        uptimeString += \`\${days}天 \`;
    }
    if (hours > 0) {
        uptimeString += \`\${hours}小时 \`;
    }
    if (minutes > 0 || (days === 0 && hours === 0)) { // Show minutes if it's the only unit or if other units are zero
        uptimeString += \`\${minutes}分钟\`;
    }

    return uptimeString.trim() || '0分钟'; // Default to 0 minutes if string is empty
}


// --- Website Status Functions ---

// Load all website statuses
async function loadAllSiteStatuses() {
    try {
        let data;
        try {
            data = await publicApiRequest('/api/sites/status');
        } catch (error) {
            // 如果获取网站状态失败，可能是数据库未初始化，尝试初始化
                        await publicApiRequest('/api/init-db');
            data = await publicApiRequest('/api/sites/status');
        }
        const sites = data.sites || [];

        const noSitesAlert = document.getElementById('noSites');
        const siteStatusTableBody = document.getElementById('siteStatusTableBody');

        if (sites.length === 0) {
            noSitesAlert.classList.remove('d-none');
            siteStatusTableBody.innerHTML = '<tr><td colspan="6" class="text-center">No websites are being monitored.</td></tr>'; // Colspan updated
            // 同时更新移动端卡片容器
            renderMobileSiteCards([]);
            return;
        } else {
            noSitesAlert.classList.add('d-none');
        }

        renderSiteStatusTable(sites);

    } catch (error) {
                const siteStatusTableBody = document.getElementById('siteStatusTableBody');
        siteStatusTableBody.innerHTML = '<tr><td colspan="6" class="text-center text-danger">Failed to load website status data. Please refresh the page.</td></tr>'; // Colspan updated
        // 显示错误通知
        showToast('danger', '加载网站数据失败，请刷新页面重试');
    }
}

// Render the website status table
async function renderSiteStatusTable(sites) {
    const tableBody = document.getElementById('siteStatusTableBody');
    tableBody.innerHTML = ''; // Clear existing rows

    for (const site of sites) {
        const row = document.createElement('tr');
        const statusInfo = getSiteStatusBadge(site.last_status);
        const lastCheckTime = site.last_checked ? new Date(site.last_checked * 1000).toLocaleString() : '从未';
        const responseTime = site.last_response_time_ms !== null ? \`\${site.last_response_time_ms} ms\` : '-';

        const historyCell = document.createElement('td');
        const historyContainer = document.createElement('div');
        historyContainer.className = 'history-bar-container';
        historyCell.appendChild(historyContainer);

        const isSiteUp = (site.last_status === 'UP' || site.last_status === 'HIGH_LATENCY');
        const nameHtml = site.name
            ? (isSiteUp
                ? \`<a href="\${site.url}" target="_blank" rel="noopener noreferrer" class="site-name-link">\${site.name}</a>\`
                : site.name)
            : '-';
        row.innerHTML = \`
            <td>\${nameHtml}</td>
            <td><span class="badge \${statusInfo.class}">\${statusInfo.text}</span></td>
            <td>\${site.last_status_code || '-'}</td>
            <td>\${responseTime}</td>
            <td>\${lastCheckTime}</td>
        \`;
        row.appendChild(historyCell);
        tableBody.appendChild(row);

        // 直接使用站点的历史数据渲染历史条
        renderSiteHistoryBar(historyContainer, site.history || []);
    }

    // 同时渲染移动端卡片
    renderMobileSiteCards(sites);
}

// Render 24h history bar for a site (unified function for PC and mobile)
function renderSiteHistoryBar(containerElement, history) {
    let historyHtml = '';
    const now = new Date();

    for (let i = 0; i < 24; i++) {
        const slotTime = new Date(now);
        slotTime.setHours(now.getHours() - i);
        const slotStart = new Date(slotTime);
        slotStart.setMinutes(0, 0, 0);
        const slotEnd = new Date(slotTime);
        slotEnd.setMinutes(59, 59, 999);

        const slotStartTimestamp = Math.floor(slotStart.getTime() / 1000);
        const slotEndTimestamp = Math.floor(slotEnd.getTime() / 1000);

        const recordForHour = history?.find(
            r => r.timestamp >= slotStartTimestamp && r.timestamp <= slotEndTimestamp
        );

        let barClass = 'history-bar-pending';
        let titleText = \`\${String(slotStart.getHours()).padStart(2, '0')}:00 - \${String((slotStart.getHours() + 1) % 24).padStart(2, '0')}:00: 无记录\`;

        if (recordForHour) {
            if (recordForHour.status === 'UP') {
                barClass = 'history-bar-up';
            } else if (recordForHour.status === 'HIGH_LATENCY') {
                barClass = 'history-bar-high-latency';
            } else if (['DOWN', 'TIMEOUT', 'ERROR'].includes(recordForHour.status)) {
                barClass = 'history-bar-down';
            }
            const recordDate = new Date(recordForHour.timestamp * 1000);
            titleText = \`\${recordDate.toLocaleString()}: \${recordForHour.status} (\${recordForHour.status_code || 'N/A'}), \${recordForHour.response_time_ms || '-'}ms\`;
        }

        historyHtml += \`<div class="history-bar \${barClass}" title="\${titleText}"></div>\`;
    }

    containerElement.innerHTML = historyHtml;
}


// ==================== LLM端点状态渲染（公开页面） ====================

async function loadAllLlmEndpointStatuses() {
    try {
        let data;
        try {
            data = await publicApiRequest('/api/llm-endpoints/status');
        } catch (error) {
            await publicApiRequest('/api/init-db');
            data = await publicApiRequest('/api/llm-endpoints/status');
        }
        const endpoints = data.endpoints || [];

        const noLlmAlert = document.getElementById('noLlmEndpoints');
        const llmStatusTableBody = document.getElementById('llmStatusTableBody');

        if (!llmStatusTableBody) return; // Not on main page

        if (endpoints.length === 0) {
            if (noLlmAlert) noLlmAlert.classList.remove('d-none');
            llmStatusTableBody.innerHTML = '<tr><td colspan="7" class="text-center">暂无LLM端点监控数据。</td></tr>';
            renderGroupedMobileLlmCards([]);
            return;
        } else {
            if (noLlmAlert) noLlmAlert.classList.add('d-none');
        }

        renderGroupedLlmStatusTable(endpoints);
    } catch (error) {
        const llmStatusTableBody = document.getElementById('llmStatusTableBody');
        if (llmStatusTableBody) {
            llmStatusTableBody.innerHTML = '<tr><td colspan="7" class="text-center text-danger">加载LLM端点数据失败，请刷新页面重试。</td></tr>';
        }
    }
}

async function renderLlmStatusTable(endpoints) {
    const tableBody = document.getElementById('llmStatusTableBody');
    if (!tableBody) return;
    tableBody.innerHTML = '';

    for (const ep of endpoints) {
        const row = document.createElement('tr');
        const statusInfo = getSiteStatusBadge(ep.last_status);
        const lastCheckTime = ep.last_checked ? new Date(ep.last_checked * 1000).toLocaleString() : '从未';
        const responseTime = ep.last_response_time_ms !== null ? \`\${ep.last_response_time_ms} ms\` : '-';
        const outputPreview = ep.last_output_preview
            ? (ep.last_output_preview.length > 60 ? ep.last_output_preview.substring(0, 60) + '...' : ep.last_output_preview)
            : '-';

        const historyCell = document.createElement('td');
        const historyContainer = document.createElement('div');
        historyContainer.className = 'history-bar-container';
        historyCell.appendChild(historyContainer);

        const displayName = ep.model;
        row.innerHTML = \`
            <td><span class="badge bg-info">\${displayName}</span></td>
            <td><span class="badge \${statusInfo.class}">\${statusInfo.text}</span></td>
            <td>\${ep.last_status_code || '-'}</td>
            <td>\${responseTime}</td>
            <td>\${lastCheckTime}</td>
            <td><small class="text-muted" title="\${ep.last_output_preview || ''}">\${outputPreview}</small></td>
        \`;
        row.appendChild(historyCell);
        tableBody.appendChild(row);

        renderSiteHistoryBar(historyContainer, ep.history || []);
    }

    renderMobileLlmCards(endpoints);
}

function renderMobileLlmCards(endpoints) {
    const container = document.getElementById('mobileLlmContainer');
    if (!container) return;

    if (!endpoints || endpoints.length === 0) {
        container.innerHTML = '<div class="text-center p-3 text-muted">暂无LLM端点监控数据。</div>';
        return;
    }

    container.innerHTML = '';
    for (const ep of endpoints) {
        const statusInfo = getSiteStatusBadge(ep.last_status);
        const lastCheckTime = ep.last_checked ? new Date(ep.last_checked * 1000).toLocaleString() : '从未';
        const responseTime = ep.last_response_time_ms !== null ? \`\${ep.last_response_time_ms} ms\` : '-';
        const displayName = ep.model;

        const card = document.createElement('div');
        card.className = 'card mb-2';

        const cardBody = document.createElement('div');
        cardBody.className = 'card-body p-2';

        cardBody.innerHTML = \`
            <div class="d-flex justify-content-between align-items-center">
                <strong>\${displayName}</strong>
                <span class="badge \${statusInfo.class}">\${statusInfo.text}</span>
            </div>
            <div class="mt-1"><small>响应: \${responseTime} | \${lastCheckTime}</small></div>
        \`;

        // 24小时历史记录 - 始终显示，即使没有数据
        const historyContainer = document.createElement('div');
        historyContainer.className = 'mobile-history-container';
        historyContainer.innerHTML = \`
            <div class="mobile-history-label">24小时记录</div>
            <div class="history-bar-container"></div>
        \`;
        cardBody.appendChild(historyContainer);

        // 使用统一的历史记录渲染函数
        const historyBarContainer = historyContainer.querySelector('.history-bar-container');
        renderSiteHistoryBar(historyBarContainer, ep.history || []);

        card.appendChild(cardBody);
        container.appendChild(card);
    }
}

async function renderGroupedLlmStatusTable(endpoints) {
    const tableBody = document.getElementById('llmStatusTableBody');
    if (!tableBody) return;

    tableBody.innerHTML = '';
    const providerGroups = groupLlmEndpointsByProvider(endpoints);

    providerGroups.forEach(group => {
        const expanded = llmProviderExpansionState[group.key] !== false;
        const providerStatus = getProviderStatusInfo(group);
        const providerHeader = getProviderHeaderMeta(group);
        const providerRow = document.createElement('tr');
        providerRow.className = 'provider-group-row';
        providerRow.innerHTML = \`
            <td colspan="7">
                <div class="d-flex justify-content-between align-items-start gap-3">
                    <div>
                        <button type="button" class="btn btn-sm btn-link text-decoration-none p-0 public-provider-toggle" data-provider-key="\${escapeHtml(group.key)}">
                            <i class="bi \${expanded ? 'bi-caret-down-fill' : 'bi-caret-right-fill'} me-1"></i>
                            <strong>\${escapeHtml(providerHeader.title)}</strong>
                        </button>
                        <div><small class="text-muted">\${escapeHtml(providerHeader.subtitle)}</small></div>
                    </div>
                    <div class="d-flex align-items-center gap-2 flex-shrink-0">
                        <small class="text-muted">\${group.endpoints.length} 个模型</small>
                        <span class="badge \${providerStatus.class}">\${providerStatus.text}</span>
                    </div>
                </div>
            </td>
        \`;
        tableBody.appendChild(providerRow);

        group.endpoints.forEach(ep => {
            const row = document.createElement('tr');
            row.dataset.providerKey = group.key;
            row.className = 'provider-child-row';
            row.style.display = expanded ? '' : 'none';

            const statusInfo = getSiteStatusBadge(ep.last_status);
            const lastCheckTime = ep.last_checked ? new Date(ep.last_checked * 1000).toLocaleString() : '从未';
            const responseTime = ep.last_response_time_ms !== null ? \`\${ep.last_response_time_ms} ms\` : '-';
            const outputPreview = ep.last_output_preview
                ? (ep.last_output_preview.length > 60 ? ep.last_output_preview.substring(0, 60) + '...' : ep.last_output_preview)
                : '-';

            const historyCell = document.createElement('td');
            const historyContainer = document.createElement('div');
            historyContainer.className = 'history-bar-container';
            historyCell.appendChild(historyContainer);

            row.innerHTML = \`
                <td><span class="ms-3 badge bg-info-subtle text-dark border">\${escapeHtml(ep.model)}</span></td>
                <td><span class="badge \${statusInfo.class}">\${statusInfo.text}</span></td>
                <td>\${ep.last_status_code || '-'}</td>
                <td>\${responseTime}</td>
                <td>\${lastCheckTime}</td>
                <td><small class="text-muted" title="\${escapeHtml(ep.last_output_preview || '')}">\${escapeHtml(outputPreview)}</small></td>
            \`;
            row.appendChild(historyCell);
            tableBody.appendChild(row);
            renderSiteHistoryBar(historyContainer, ep.history || []);
        });
    });

    tableBody.querySelectorAll('.public-provider-toggle').forEach(btn => {
        btn.addEventListener('click', function() {
            const providerKey = this.getAttribute('data-provider-key');
            llmProviderExpansionState[providerKey] = !(llmProviderExpansionState[providerKey] !== false);
            renderGroupedLlmStatusTable(endpoints);
            renderGroupedMobileLlmCards(endpoints);
        });
    });

    renderGroupedMobileLlmCards(endpoints);
}

function renderGroupedMobileLlmCards(endpoints) {
    const container = document.getElementById('mobileLlmContainer');
    if (!container) return;

    if (!endpoints || endpoints.length === 0) {
        container.innerHTML = '<div class="text-center p-3 text-muted">暂无LLM端点监控数据。</div>';
        return;
    }

    container.innerHTML = '';
    const providerGroups = groupLlmEndpointsByProvider(endpoints);

    providerGroups.forEach(group => {
        const expanded = llmProviderExpansionState[group.key] !== false;
        const providerStatus = getProviderStatusInfo(group);
        const providerHeader = getProviderHeaderMeta(group);
        const card = document.createElement('div');
        card.className = 'card mb-2';

        const cardBody = document.createElement('div');
        cardBody.className = 'card-body p-2';
        cardBody.innerHTML = \`
            <div class="d-flex justify-content-between align-items-center">
                <button type="button" class="btn btn-sm btn-link text-decoration-none p-0 public-provider-toggle" data-provider-key="\${escapeHtml(group.key)}">
                    <i class="bi \${expanded ? 'bi-caret-down-fill' : 'bi-caret-right-fill'} me-1"></i><strong>\${escapeHtml(providerHeader.title)}</strong>
                </button>
                <span class="badge \${providerStatus.class}">\${providerStatus.text}</span>
            </div>
            <div class="mt-1 mb-2"><small class="text-muted">\${escapeHtml(providerHeader.subtitle)}</small></div>
        \`;

        group.endpoints.forEach(ep => {
            const statusInfo = getSiteStatusBadge(ep.last_status);
            const lastCheckTime = ep.last_checked ? new Date(ep.last_checked * 1000).toLocaleString() : '从未';
            const responseTime = ep.last_response_time_ms !== null ? \`\${ep.last_response_time_ms} ms\` : '-';
            const item = document.createElement('div');
            item.dataset.providerKey = group.key;
            item.style.display = expanded ? '' : 'none';
            item.className = 'border-top pt-2 mt-2';
            item.innerHTML = \`
                <div class="d-flex justify-content-between align-items-center">
                    <strong>\${escapeHtml(ep.model)}</strong>
                    <span class="badge \${statusInfo.class}">\${statusInfo.text}</span>
                </div>
                <div class="mt-1"><small>响应: \${responseTime} | \${lastCheckTime}</small></div>
                <div class="mobile-history-container mt-2">
                    <div class="mobile-history-label">24小时记录</div>
                    <div class="history-bar-container"></div>
                </div>
            \`;
            cardBody.appendChild(item);
            renderSiteHistoryBar(item.querySelector('.history-bar-container'), ep.history || []);
        });

        card.appendChild(cardBody);
        container.appendChild(card);
    });

    container.querySelectorAll('.public-provider-toggle').forEach(btn => {
        btn.addEventListener('click', function() {
            const providerKey = this.getAttribute('data-provider-key');
            llmProviderExpansionState[providerKey] = !(llmProviderExpansionState[providerKey] !== false);
            renderGroupedMobileLlmCards(endpoints);
            renderGroupedLlmStatusTable(endpoints);
        });
    });
}

// Get website status badge class and text (copied from admin.js for reuse)
function getSiteStatusBadge(status) {
    switch (status) {
        case 'UP': return { class: 'bg-success', text: '正常' };
        case 'HIGH_LATENCY': return { class: 'bg-warning text-dark', text: '高延迟' };
        case 'DOWN': return { class: 'bg-danger', text: '故障' };
        case 'TIMEOUT': return { class: 'bg-warning text-dark', text: '超时' };
        case 'ERROR': return { class: 'bg-danger', text: '错误' };
        case 'PENDING': return { class: 'bg-secondary', text: '待检测' };
        default: return { class: 'bg-secondary', text: '未知' };
    }
}

// ==================== 全局背景设置功能 ====================

// 全局背景设置加载函数
async function loadGlobalBackgroundSettings() {
    try {
        // 检查localStorage缓存（无痕模式兼容）
        const cacheKey = 'background-settings-cache';
        let cached = null;
        let settings = null;

        try {
            cached = localStorage.getItem(cacheKey);
        } catch (storageError) {
                    }

        if (cached) {
            try {
                const cachedData = JSON.parse(cached);
                const now = Date.now();
                const cacheAge = now - cachedData.timestamp;
                const CACHE_DURATION = 5 * 60 * 1000; // 5分钟缓存

                if (cacheAge < CACHE_DURATION) {
                    settings = cachedData;
                                    }
            } catch (parseError) {
                            }
        }

        // 缓存过期或不存在，从API获取
        if (!settings) {
            try {
                const response = await fetch('/api/background-settings');
                if (response.ok) {
                    const apiSettings = await response.json();
                    settings = {
                        enabled: apiSettings.enabled,
                        url: apiSettings.url,
                        opacity: apiSettings.opacity,
                        timestamp: Date.now()
                    };

                    // 尝试更新缓存（无痕模式可能失败，但不影响功能）
                    try {
                        localStorage.setItem(cacheKey, JSON.stringify(settings));
                                            } catch (storageError) {
                                            }
                } else {
                                        settings = { enabled: false, url: '', opacity: 80 };
                }
            } catch (error) {
                                settings = { enabled: false, url: '', opacity: 80 };
            }
        }

        // 应用背景设置
        applyGlobalBackgroundSettings(settings.enabled, settings.url, settings.opacity);

    } catch (error) {
            }
}

// 应用全局背景设置
function applyGlobalBackgroundSettings(enabled, url, opacity) {
    const body = document.body;

    if (enabled && url) {
        // 验证URL格式
        if (!url.startsWith('https://')) {
                        return;
        }

        // 预加载图片，确保加载成功
        const img = new Image();
        img.onload = function() {
            // 图片加载成功，应用背景
            body.style.setProperty('--custom-background-url', \`url(\${url})\`);
            body.style.setProperty('--page-opacity', opacity / 100);
            body.classList.add('custom-background-enabled');



                    };
        img.onerror = function() {
            // 图片加载失败，不应用背景
            body.classList.remove('custom-background-enabled');
            body.classList.remove('low-contrast', 'medium-contrast', 'high-contrast');
        };
        img.src = url;
    } else {
        // 移除背景设置
        body.style.removeProperty('--custom-background-url');
        body.style.removeProperty('--page-opacity');
        body.classList.remove('custom-background-enabled');
            }
}



// 页面加载时初始化背景设置
document.addEventListener('DOMContentLoaded', function() {
    loadGlobalBackgroundSettings();
});

// 监听storage事件，实现跨页面设置同步
window.addEventListener('storage', function(e) {
    if (e.key === 'background-settings-cache' && e.newValue) {
        try {
            const newSettings = JSON.parse(e.newValue);
            applyGlobalBackgroundSettings(newSettings.enabled, newSettings.url, newSettings.opacity);
                    } catch (error) {
                    }
    }
});
`;
}

function getLoginJs() {
  return `// login.js - 登录页面的JavaScript逻辑

// ==================== 统一API请求工具 ====================
// 注意：此处的apiRequest函数已移至主要位置，避免重复定义

// --- Theme Management (copied from main.js) ---
const THEME_KEY = 'vps-monitor-theme';
const LIGHT_THEME = 'light';
const DARK_THEME = 'dark';

function initializeTheme() {
    const themeToggler = document.getElementById('themeToggler');
    if (!themeToggler) return;

    const storedTheme = localStorage.getItem(THEME_KEY) || LIGHT_THEME;
    applyTheme(storedTheme);

    themeToggler.addEventListener('click', () => {
        const currentTheme = document.documentElement.getAttribute('data-bs-theme');
        const newTheme = currentTheme === DARK_THEME ? LIGHT_THEME : DARK_THEME;
        applyTheme(newTheme);
        localStorage.setItem(THEME_KEY, newTheme);
    });
}

function applyTheme(theme) {
    document.documentElement.setAttribute('data-bs-theme', theme);
    const themeTogglerIcon = document.querySelector('#themeToggler i');
    if (themeTogglerIcon) {
        if (theme === DARK_THEME) {
            themeTogglerIcon.classList.remove('bi-moon-stars-fill');
            themeTogglerIcon.classList.add('bi-sun-fill');
        } else {
            themeTogglerIcon.classList.remove('bi-sun-fill');
            themeTogglerIcon.classList.add('bi-moon-stars-fill');
        }
    }
}
// --- End Theme Management ---


// 页面加载完成后执行
document.addEventListener('DOMContentLoaded', function() {
    // Initialize theme
    initializeTheme();

    // 获取登录表单元素
    const loginForm = document.getElementById('loginForm');
    const loginAlert = document.getElementById('loginAlert');

    // 添加表单提交事件监听
    loginForm.addEventListener('submit', function(e) {
        e.preventDefault();

        // 获取用户输入
        const username = document.getElementById('username').value.trim();
        const password = document.getElementById('password').value.trim();

        // 验证输入
        if (!username || !password) {
            showToast('warning', '请输入用户名和密码');
            return;
        }

        // 执行登录
        login(username, password);
    });

    // 加载默认凭据信息
    loadDefaultCredentials();

    // 检查是否已登录
    checkLoginStatus();
});

// ==================== 统一API请求工具 ====================

// 获取认证头
function getAuthHeaders() {
    const token = localStorage.getItem('auth_token');
    const headers = { 'Content-Type': 'application/json' };
    if (token) {
        headers['Authorization'] = 'Bearer ' + token;
    }
    return headers;
}

// 统一API请求函数
async function apiRequest(url, options = {}) {
    const defaultOptions = {
        headers: getAuthHeaders(),
        ...options
    };

    try {
        const response = await fetch(url, defaultOptions);

        // 处理认证失败
        if (response.status === 401) {
            localStorage.removeItem('auth_token');
            if (window.location.pathname !== '/login.html') {
                window.location.href = 'login.html';
            }
            throw new Error('认证失败，请重新登录');
        }

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.message || \`请求失败 (\${response.status})\`);
        }

        return await response.json();
    } catch (error) {
                throw error;
    }
}

// 加载默认凭据信息（本地显示，无需API调用）
function loadDefaultCredentials() {
    const credentialsInfo = document.getElementById('defaultCredentialsInfo');
    if (credentialsInfo) {
        credentialsInfo.innerHTML = '默认账号密码: <strong>admin</strong> / <strong>monitor2025!</strong><br><small class="text-danger fw-bold">建议首次登录后修改密码</small>';
    }
}

// 检查登录状态
async function checkLoginStatus() {
    try {
        // 从localStorage获取token
        const token = localStorage.getItem('auth_token');
        if (!token) {
            return;
        }

        const data = await apiRequest('/api/auth/status');

        if (data.authenticated) {
            // 已登录，重定向到管理后台
            window.location.href = 'admin.html';
        }
    } catch (error) {
            }
}

// 登录函数
async function login(username, password) {
    try {
        // 显示加载状态
        const loginForm = document.getElementById('loginForm');
        const submitBtn = loginForm.querySelector('button[type="submit"]');
        const originalBtnText = submitBtn.innerHTML;
        submitBtn.disabled = true;
        submitBtn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> 登录中...';

        // 发送登录请求（不需要认证头）
        const response = await fetch('/api/auth/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password })
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.message || \`登录失败 (\${response.status})\`);
        }

        const data = await response.json();

        // 恢复按钮状态
        submitBtn.disabled = false;
        submitBtn.innerHTML = originalBtnText;

        // 保存token到localStorage
        localStorage.setItem('auth_token', data.token);

        // 直接跳转到管理后台
        window.location.href = 'admin.html';

    } catch (error) {
                // 恢复按钮状态
        const loginForm = document.getElementById('loginForm');
        const submitBtn = loginForm.querySelector('button[type="submit"]');
        submitBtn.disabled = false;
        submitBtn.innerHTML = '登录';

        showToast('danger', error.message || '登录请求失败，请稍后重试');
    }
}



// ==================== 全局背景设置功能 ====================

// 全局背景设置加载函数（登录页面版本）
async function loadGlobalBackgroundSettings() {
    try {
        // 检查localStorage缓存（无痕模式兼容）
        const cacheKey = 'background-settings-cache';
        let cached = null;
        let settings = null;

        try {
            cached = localStorage.getItem(cacheKey);
        } catch (storageError) {
                    }

        if (cached) {
            try {
                const cachedData = JSON.parse(cached);
                const now = Date.now();
                const cacheAge = now - cachedData.timestamp;
                const CACHE_DURATION = 5 * 60 * 1000; // 5分钟缓存

                if (cacheAge < CACHE_DURATION) {
                    settings = cachedData;
                                    }
            } catch (parseError) {
                            }
        }

        // 缓存过期或不存在，从API获取
        if (!settings) {
            try {
                const response = await fetch('/api/background-settings');
                if (response.ok) {
                    const apiSettings = await response.json();
                    settings = {
                        enabled: apiSettings.enabled,
                        url: apiSettings.url,
                        opacity: apiSettings.opacity,
                        timestamp: Date.now()
                    };

                    // 尝试更新缓存（无痕模式可能失败，但不影响功能）
                    try {
                        localStorage.setItem(cacheKey, JSON.stringify(settings));
                                            } catch (storageError) {
                                            }
                } else {
                                        settings = { enabled: false, url: '', opacity: 80 };
                }
            } catch (error) {
                                settings = { enabled: false, url: '', opacity: 80 };
            }
        }

        // 应用背景设置
        applyGlobalBackgroundSettings(settings.enabled, settings.url, settings.opacity);

    } catch (error) {
            }
}

// 应用全局背景设置
function applyGlobalBackgroundSettings(enabled, url, opacity) {
    const body = document.body;

    if (enabled && url) {
        // 验证URL格式
        if (!url.startsWith('https://')) {
                        return;
        }

        // 预加载图片，确保加载成功
        const img = new Image();
        img.onload = function() {
            // 图片加载成功，应用背景
            body.style.setProperty('--custom-background-url', \`url(\${url})\`);
            body.style.setProperty('--page-opacity', opacity / 100);
            body.classList.add('custom-background-enabled');



                    };
        img.onerror = function() {
            // 图片加载失败，不应用背景
            body.classList.remove('custom-background-enabled');
        };
        img.src = url;
    } else {
        // 移除背景设置
        body.style.removeProperty('--custom-background-url');
        body.style.removeProperty('--page-opacity');
        body.classList.remove('custom-background-enabled');
            }
}



// 页面加载时初始化背景设置
document.addEventListener('DOMContentLoaded', function() {
    loadGlobalBackgroundSettings();
});

// 监听storage事件，实现跨页面设置同步
window.addEventListener('storage', function(e) {
    if (e.key === 'background-settings-cache' && e.newValue) {
        try {
            const newSettings = JSON.parse(e.newValue);
            applyGlobalBackgroundSettings(newSettings.enabled, newSettings.url, newSettings.opacity);
                    } catch (error) {
                    }
    }
});
`;
}
// Helper functions for updating server/site settings are no longer needed for frequent notifications
// as that feature is removed.

function getAdminJs() {
  return `// admin.js - 管理后台的JavaScript逻辑

// ==================== 统一API请求工具 ====================

// 获取认证头
function getAuthHeaders() {
    const token = localStorage.getItem('auth_token');
    const headers = { 'Content-Type': 'application/json' };
    if (token) {
        headers['Authorization'] = 'Bearer ' + token;
    }
    return headers;
}

// 统一API请求函数
async function apiRequest(url, options = {}) {
    const defaultOptions = {
        headers: getAuthHeaders(),
        ...options
    };

    try {
        const response = await fetch(url, defaultOptions);

        // 处理认证失败
        if (response.status === 401) {
            localStorage.removeItem('auth_token');
            window.location.href = 'login.html';
            throw new Error('认证失败，请重新登录');
        }

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.message || \`请求失败 (\${response.status})\`);
        }

        return await response.json();
    } catch (error) {
                throw error;
    }
}

// Global variables for VPS data updates
let vpsUpdateInterval = null;
const DEFAULT_VPS_REFRESH_INTERVAL_MS = 60000; // Default to 60 seconds for VPS data if backend setting fails
const llmProviderExpansionState = {};

function escapeHtml(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function getProviderFallbackName(apiUrl) {
    if (!apiUrl) return '未命名 Provider';
    try {
        const parsed = new URL(apiUrl);
        return parsed.hostname.replace(/^www\./, '') || apiUrl;
    } catch (error) {
        return apiUrl;
    }
}

function groupLlmEndpointsByProvider(endpoints) {
    const groups = [];
    const groupMap = new Map();

    endpoints.forEach((endpoint) => {
        const key = endpoint.api_url || endpoint.id;
        let group = groupMap.get(key);
        if (!group) {
            group = {
                key,
                api_url: endpoint.api_url,
                providerName: (endpoint.name || '').trim(),
                endpoints: []
            };
            groupMap.set(key, group);
            groups.push(group);
        }
        if (endpoint.name && endpoint.name.trim()) {
            group.providerName = endpoint.name.trim();
        }
        group.endpoints.push(endpoint);
    });

    groups.forEach((group) => {
        if (!group.providerName) {
            group.providerName = getProviderFallbackName(group.api_url);
        }
        if (typeof llmProviderExpansionState[group.key] === 'undefined') {
            llmProviderExpansionState[group.key] = true;
        }
    });

    return groups;
}

function getProviderStatusInfo(group) {
    const statuses = group.endpoints.map(ep => ep.last_status);
    if (statuses.some(status => ['DOWN', 'ERROR'].includes(status))) return getSiteStatusBadge('DOWN');
    if (statuses.includes('TIMEOUT')) return getSiteStatusBadge('TIMEOUT');
    if (statuses.includes('HIGH_LATENCY')) return getSiteStatusBadge('HIGH_LATENCY');
    if (statuses.includes('UP')) return getSiteStatusBadge('UP');
    return getSiteStatusBadge('PENDING');
}

// Function to fetch VPS refresh interval and start periodic VPS data updates
async function initializeVpsDataUpdates() {
        let vpsRefreshIntervalMs = DEFAULT_VPS_REFRESH_INTERVAL_MS;

    try {
                const data = await apiRequest('/api/admin/settings/vps-report-interval');
                if (data && typeof data.interval === 'number' && data.interval > 0) {
            vpsRefreshIntervalMs = data.interval * 1000; // Convert seconds to milliseconds
                    } else {
            // 使用默认值
        }
    } catch (error) {
            }

    // Clear existing interval if any
    if (vpsUpdateInterval) {
                clearInterval(vpsUpdateInterval);
    }

    // Set up new periodic updates for VPS data ONLY
        vpsUpdateInterval = setInterval(() => {
                // Reload server list to get updated data
        if (typeof loadServerList === 'function') {
            loadServerList();
        }
    }, vpsRefreshIntervalMs);

    }

// --- Theme Management (copied from main.js) ---
const THEME_KEY = 'vps-monitor-theme';
const LIGHT_THEME = 'light';
const DARK_THEME = 'dark';

function initializeTheme() {
    const themeToggler = document.getElementById('themeToggler');
    if (!themeToggler) return;

    const storedTheme = localStorage.getItem(THEME_KEY) || LIGHT_THEME;
    applyTheme(storedTheme);

    themeToggler.addEventListener('click', () => {
        const currentTheme = document.documentElement.getAttribute('data-bs-theme');
        const newTheme = currentTheme === DARK_THEME ? LIGHT_THEME : DARK_THEME;
        applyTheme(newTheme);
        localStorage.setItem(THEME_KEY, newTheme);
    });
}

function applyTheme(theme) {
    document.documentElement.setAttribute('data-bs-theme', theme);
    const themeTogglerIcon = document.querySelector('#themeToggler i');
    if (themeTogglerIcon) {
        if (theme === DARK_THEME) {
            themeTogglerIcon.classList.remove('bi-moon-stars-fill');
            themeTogglerIcon.classList.add('bi-sun-fill');
        } else {
            themeTogglerIcon.classList.remove('bi-sun-fill');
            themeTogglerIcon.classList.add('bi-moon-stars-fill');
        }
    }
}
// --- End Theme Management ---

// 工具提示现在使用浏览器原生title属性，无需JavaScript初始化

// 优化的清理函数 - 清理可能卡住的开关
function cleanupStuckToggles() {
    const stuckToggles = document.querySelectorAll('[data-updating="true"]');
    if (stuckToggles.length > 0) {
                stuckToggles.forEach(toggle => {
            toggle.disabled = false;
            delete toggle.dataset.updating;
            toggle.style.opacity = '1';
        });
    }
}

// 移除了复杂的waitForToggleReady函数，现在直接在API响应后更新UI状态

// 全局变量
let currentServerId = null;
let currentSiteId = null; // For site deletion
let serverList = [];
let siteList = []; // For monitored sites
let currentLlmEndpointId = null; // For LLM endpoint deletion
let llmEndpointList = []; // For monitored LLM endpoints
let hasAddedNewServer = false; // 标记是否添加了新服务器

// 页面加载完成后执行
document.addEventListener('DOMContentLoaded', async function() {
    // Initialize theme
    initializeTheme();

    // 检查登录状态 - 必须先完成认证检查
    await checkLoginStatus();

    // 初始化事件监听
    initEventListeners();

    // 初始化Bootstrap tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // 加载服务器列表
    loadServerList();
    // 加载监控网站列表
    loadSiteList();
    // 加载LLM端点监控列表
    loadLlmEndpointList();
    // 加载Telegram设置
    loadTelegramSettings();
    // 加载ntfy设置
    loadNtfySettings();
    // 加载背景设置
    loadBackgroundSettings();
    // 加载全局设置 (VPS Report Interval) - will use serverAlert for notifications
    loadGlobalSettings();

    // 初始化管理后台的定时刷新机制
    initializeVpsDataUpdates();

    // 检查是否使用默认密码
    checkDefaultPasswordUsage();

    // 优化：停止自动清理以节省配额
    // setInterval(cleanupStuckToggles, 30000);
    });

// 检查登录状态
async function checkLoginStatus() {
    try {
        // 从localStorage获取token
        const token = localStorage.getItem('auth_token');
        if (!token) {
            // 未登录，重定向到登录页面
            window.location.href = 'login.html';
            return;
        }

        const data = await apiRequest('/api/auth/status');
        if (!data.authenticated) {
            // 未登录，重定向到登录页面
            window.location.href = 'login.html';
        }
    } catch (error) {
                window.location.href = 'login.html';
    }
}

// 检查是否使用默认密码
async function checkDefaultPasswordUsage() {
    try {
        // 从localStorage获取是否显示过默认密码提醒
        const hasShownDefaultPasswordWarning = localStorage.getItem('hasShownDefaultPasswordWarning');

        if (hasShownDefaultPasswordWarning === 'true') {
            return; // 已经显示过提醒，不再显示
        }

        // 检查当前用户登录状态和默认密码使用情况
        const token = localStorage.getItem('auth_token');
                if (token) {
            try {
                const statusData = await apiRequest('/api/auth/status');
                if (statusData.authenticated && statusData.user && statusData.user.usingDefaultPassword) {
                    // 显示默认密码提醒
                    showToast('warning',
                        '安全提醒：您正在使用默认密码登录。为了您的账户安全，建议尽快修改密码。点击右上角的"修改密码"按钮来更改密码。',
                        { duration: 10000 }); // 10秒显示

                    // 标记已显示过提醒
                    localStorage.setItem('hasShownDefaultPasswordWarning', 'true');
                }
            } catch (error) {
                            }
        }
    } catch (error) {
            }
}

// 初始化事件监听
function initEventListeners() {
    // 添加服务器按钮
    document.getElementById('addServerBtn').addEventListener('click', function() {
        showServerModal();
    });

    // 保存服务器按钮
    document.getElementById('saveServerBtn').addEventListener('click', function() {
        saveServer();
    });

    // Helper function for copying text to clipboard and providing button feedback
    function copyToClipboard(textToCopy, buttonElement) {
        navigator.clipboard.writeText(textToCopy).then(() => {
            const originalHtml = buttonElement.innerHTML;
            buttonElement.innerHTML = '<i class="bi bi-check-lg"></i>'; // Using a larger check icon
            buttonElement.classList.add('btn-success');
            buttonElement.classList.remove('btn-outline-secondary');

            setTimeout(() => {
                buttonElement.innerHTML = originalHtml;
                buttonElement.classList.remove('btn-success');
                buttonElement.classList.add('btn-outline-secondary');
            }, 2000);
        }).catch(err => {
            // 静默处理复制失败
            const originalHtml = buttonElement.innerHTML;
            buttonElement.innerHTML = '<i class="bi bi-x-lg"></i>'; // Error icon
            buttonElement.classList.add('btn-danger');
            buttonElement.classList.remove('btn-outline-secondary');
            setTimeout(() => {
                buttonElement.innerHTML = originalHtml;
                buttonElement.classList.remove('btn-danger');
                buttonElement.classList.add('btn-outline-secondary');
            }, 2000);
        });
    }

    // 复制API密钥按钮
    document.getElementById('copyApiKeyBtn').addEventListener('click', function() {
        const apiKeyInput = document.getElementById('apiKey');
        copyToClipboard(apiKeyInput.value, this);
    });

    // 复制服务器ID按钮
    document.getElementById('copyServerIdBtn').addEventListener('click', function() {
        const serverIdInput = document.getElementById('serverIdDisplay');
        copyToClipboard(serverIdInput.value, this);
    });

    // 复制Worker地址按钮
    document.getElementById('copyWorkerUrlBtn').addEventListener('click', function() {
        const workerUrlInput = document.getElementById('workerUrlDisplay');
        copyToClipboard(workerUrlInput.value, this);
    });

    // 确认删除按钮
    document.getElementById('confirmDeleteBtn').addEventListener('click', function() {
        if (currentServerId) {
            deleteServer(currentServerId);
        }
    });

    // 修改密码按钮（移动端）
    document.getElementById('changePasswordBtn').addEventListener('click', function() {
        showPasswordModal();
    });

    // 修改密码按钮（PC端）
    document.getElementById('changePasswordBtnDesktop').addEventListener('click', function() {
        showPasswordModal();
    });

    // 保存密码按钮
    document.getElementById('savePasswordBtn').addEventListener('click', function() {
        changePassword();
    });

    // 退出登录按钮
    document.getElementById('logoutBtn').addEventListener('click', function() {
        logout();
    });

    // --- Site Monitoring Event Listeners ---
    document.getElementById('addSiteBtn').addEventListener('click', function() {
        showSiteModal();
    });

    document.getElementById('saveSiteBtn').addEventListener('click', function() {
        saveSite();
    });

     document.getElementById('confirmDeleteSiteBtn').addEventListener('click', function() {
        if (currentSiteId) {
            deleteSite(currentSiteId);
        }
    });

    // --- LLM Endpoint Monitoring Event Listeners ---
    document.getElementById('addLlmEndpointBtn').addEventListener('click', function() {
        showLlmEndpointModal();
    });

    document.getElementById('saveLlmEndpointBtn').addEventListener('click', function() {
        saveLlmEndpoint();
    });

    document.getElementById('confirmDeleteLlmEndpointBtn').addEventListener('click', function() {
        if (currentLlmEndpointId) {
            deleteLlmEndpoint(currentLlmEndpointId);
        }
    });

    document.getElementById('saveLlmCheckIntervalBtn').addEventListener('click', function() {
        saveLlmCheckInterval();
    });

    document.getElementById('saveHighLatencyThresholdsBtn').addEventListener('click', function() {
        saveHighLatencyThresholds();
    });

    // 加载LLM检查频率设置
    loadLlmCheckInterval();
    loadHighLatencyThresholds();

    // 保存Telegram设置按钮
    document.getElementById('saveTelegramSettingsBtn').addEventListener('click', function() {
        saveTelegramSettings();
    });

    // 保存ntfy设置按钮
    document.getElementById('saveNtfySettingsBtn').addEventListener('click', function() {
        saveNtfySettings();
    });

    // Background Settings Event Listeners
    document.getElementById('saveBackgroundSettingsBtn').addEventListener('click', function() {
        saveBackgroundSettings();
    });

    // 透明度滑块实时预览
    document.getElementById('pageOpacity').addEventListener('input', function() {
        updateOpacityPreview();
    });

    // 背景开关变化时的预览
    document.getElementById('enableCustomBackground').addEventListener('change', function() {
        const enabled = this.checked;
        const url = document.getElementById('backgroundImageUrl').value.trim();
        const opacity = parseInt(document.getElementById('pageOpacity').value, 10);
        applyBackgroundSettings(enabled, url, opacity, false);
    });

    // URL输入框变化时的预览
    document.getElementById('backgroundImageUrl').addEventListener('input', function() {
        const enabled = document.getElementById('enableCustomBackground').checked;
        const url = this.value.trim();
        const opacity = parseInt(document.getElementById('pageOpacity').value, 10);
        if (enabled) {
            applyBackgroundSettings(enabled, url, opacity, false);
        }
    });

    // Global Settings Event Listener
    document.getElementById('saveVpsReportIntervalBtn').addEventListener('click', function() {
        saveVpsReportInterval();
    });

    // 服务器模态框关闭事件监听器
    const serverModal = document.getElementById('serverModal');
    if (serverModal) {
        serverModal.addEventListener('hidden.bs.modal', function() {
            // 检查是否有新添加的服务器需要刷新列表
            if (hasAddedNewServer) {
                hasAddedNewServer = false; // 重置标记
                loadServerList(); // 刷新服务器列表
            }
        });
    }

    // 初始化排序下拉菜单默认选择
    setTimeout(() => {
        // 确保DOM已完全加载
        updateServerSortDropdownSelection('custom');
        updateSiteSortDropdownSelection('custom');
    }, 100);
}

// --- Server Management Functions ---

// 加载服务器列表
async function loadServerList() {
    try {
        const data = await apiRequest('/api/admin/servers');
        serverList = data.servers || [];

        // 简化逻辑：直接渲染，智能状态显示会处理更新中的按钮
        renderServerTable(serverList);
    } catch (error) {
                showToast('danger', '加载服务器列表失败，请刷新页面重试');
    }
}

// 渲染服务器表格
function renderServerTable(servers) {
    const tableBody = document.getElementById('serverTableBody');

    // 简化状态管理：不再需要复杂的状态保存机制

    tableBody.innerHTML = '';

    if (servers.length === 0) {
        const row = document.createElement('tr');
        row.innerHTML = '<td colspan="10" class="text-center">暂无服务器数据</td>'; // Updated colspan
        tableBody.appendChild(row);
        // 同时更新移动端卡片
        renderMobileAdminServerCards([]);
        return;
    }

    servers.forEach((server, index) => {
        const row = document.createElement('tr');
        row.setAttribute('data-server-id', server.id);
        row.classList.add('server-row-draggable');
        row.draggable = true;

        // 格式化最后更新时间
        let lastUpdateText = '从未';
        let statusBadge = '<span class="badge bg-secondary">未知</span>';

        if (server.last_report) {
            const lastUpdate = new Date(server.last_report * 1000);
            lastUpdateText = lastUpdate.toLocaleString();

            // 检查是否在线（最后报告时间在5分钟内）
            const now = new Date();
            const diffMinutes = (now - lastUpdate) / (1000 * 60);

            if (diffMinutes <= 5) {
                statusBadge = '<span class="badge bg-success">在线</span>';
            } else {
                statusBadge = '<span class="badge bg-danger">离线</span>';
            }
        }

        // 智能状态显示：完整保存更新中按钮的所有状态
        const existingToggle = document.querySelector('.server-visibility-toggle[data-server-id="' + server.id + '"]');
        const isCurrentlyUpdating = existingToggle && existingToggle.dataset.updating === 'true';
        const displayState = isCurrentlyUpdating ? existingToggle.checked : server.is_public;
        const needsUpdatingState = isCurrentlyUpdating;

        row.innerHTML =
            '<td>' +
                '<div class="btn-group">' +
                    '<i class="bi bi-grip-vertical text-muted me-2" style="cursor: grab;" title="拖拽排序"></i>' +
                     '<button class="btn btn-sm btn-outline-secondary move-server-btn" data-id="' + server.id + '" data-direction="up" ' + (index === 0 ? 'disabled' : '') + '>' +
                        '<i class="bi bi-arrow-up"></i>' +
                    '</button>' +
                     '<button class="btn btn-sm btn-outline-secondary move-server-btn" data-id="' + server.id + '" data-direction="down" ' + (index === servers.length - 1 ? 'disabled' : '') + '>' +
                        '<i class="bi bi-arrow-down"></i>' +
                    '</button>' +
                '</div>' +
            '</td>' +
            '<td>' + server.id + '</td>' +
            '<td>' + server.name + '</td>' +
            '<td>' + (server.description || '-') + '</td>' +
            '<td>' + statusBadge + '</td>' +
            '<td>' + lastUpdateText + '</td>' +
            '<td>' +
                '<button class="btn btn-sm btn-outline-secondary view-key-btn" data-id="' + server.id + '">' +
                    '<i class="bi bi-key"></i> 查看密钥' +
                '</button>' +
            '</td>' +
            '<td>' +
                '<button class="btn btn-sm btn-outline-info copy-vps-script-btn" data-id="' + server.id + '" data-name="' + server.name + '" title="复制VPS安装脚本">' +
                    '<i class="bi bi-clipboard-plus"></i> 复制脚本' +
                '</button>' +
            '</td>' +
            '<td>' +
                '<div class="form-check form-switch">' +
                    '<input class="form-check-input server-visibility-toggle" type="checkbox" data-server-id="' + server.id + '" ' + (displayState ? 'checked' : '') + (needsUpdatingState ? ' data-updating="true"' : '') + '>' +
                '</div>' +
            '</td>' +
            '<td>' +
                '<div class="btn-group">' +
                    '<button class="btn btn-sm btn-outline-primary edit-server-btn" data-id="' + server.id + '">' +
                        '<i class="bi bi-pencil"></i>' +
                    '</button>' +
                    '<button class="btn btn-sm btn-outline-danger delete-server-btn" data-id="' + server.id + '" data-name="' + server.name + '">' +
                        '<i class="bi bi-trash"></i>' +
                    '</button>' +
                '</div>' +
            '</td>';

        tableBody.appendChild(row);
    });

    // 初始化拖拽排序
    initializeServerDragSort();

    // 添加事件监听
    document.querySelectorAll('.view-key-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const serverId = this.getAttribute('data-id');
            viewApiKey(serverId);
        });
    });

    document.querySelectorAll('.edit-server-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const serverId = this.getAttribute('data-id');
            editServer(serverId);
        });
    });

    document.querySelectorAll('.delete-server-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const serverId = this.getAttribute('data-id');
            const serverName = this.getAttribute('data-name');
            showDeleteConfirmation(serverId, serverName);
        });
    });

    document.querySelectorAll('.move-server-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const serverId = this.getAttribute('data-id');
            const direction = this.getAttribute('data-direction');
            moveServer(serverId, direction);
        });
    });

    document.querySelectorAll('.copy-vps-script-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const serverId = this.getAttribute('data-id');
            const serverName = this.getAttribute('data-name');
            copyVpsInstallScript(serverId, serverName, this);
        });
    });

    // 优化的显示开关事件监听 - 直接处理状态切换
    document.querySelectorAll('.server-visibility-toggle').forEach(toggle => {
        toggle.addEventListener('click', function(event) {
            // 如果开关正在更新中，忽略点击
            if (this.disabled || this.dataset.updating === 'true') {
                event.preventDefault();
                return;
            }

            const serverId = this.getAttribute('data-server-id');
            const targetState = this.checked; // 点击后的状态就是目标状态
            const originalState = !this.checked; // 原始状态是目标状态的相反

                        // 立即设置为加载状态
            this.disabled = true;
            this.style.opacity = '0.6';
            this.dataset.updating = 'true';

            updateServerVisibility(serverId, targetState, originalState, this);
        });
    });

    // 重新应用正在更新按钮的视觉状态（因为重新渲染会创建新元素）
    document.querySelectorAll('.server-visibility-toggle[data-updating="true"]').forEach(toggle => {
        toggle.disabled = true;
        toggle.style.opacity = '0.6';
    });

    // 同时渲染移动端卡片
    renderMobileAdminServerCards(servers);
}

// 初始化服务器拖拽排序
function initializeServerDragSort() {
    const tableBody = document.getElementById('serverTableBody');
    if (!tableBody) return;

    let draggedElement = null;
    let draggedOverElement = null;

    // 为所有可拖拽行添加事件监听
    const draggableRows = tableBody.querySelectorAll('.server-row-draggable');

    draggableRows.forEach(row => {
        row.addEventListener('dragstart', function(e) {
            draggedElement = this;
            this.style.opacity = '0.5';
            e.dataTransfer.effectAllowed = 'move';
            e.dataTransfer.setData('text/html', this.outerHTML);
        });

        row.addEventListener('dragend', function(e) {
            this.style.opacity = '';
            draggedElement = null;
            draggedOverElement = null;

            // 移除所有拖拽样式
            draggableRows.forEach(r => {
                r.classList.remove('drag-over-top', 'drag-over-bottom');
            });
        });

        row.addEventListener('dragover', function(e) {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';

            if (this === draggedElement) return;

            draggedOverElement = this;

            // 移除其他行的拖拽样式
            draggableRows.forEach(r => {
                if (r !== this) {
                    r.classList.remove('drag-over-top', 'drag-over-bottom');
                }
            });

            // 确定插入位置
            const rect = this.getBoundingClientRect();
            const midpoint = rect.top + rect.height / 2;

            if (e.clientY < midpoint) {
                this.classList.add('drag-over-top');
                this.classList.remove('drag-over-bottom');
            } else {
                this.classList.add('drag-over-bottom');
                this.classList.remove('drag-over-top');
            }
        });

        row.addEventListener('drop', function(e) {
            e.preventDefault();

            if (this === draggedElement) return;

            const draggedServerId = draggedElement.getAttribute('data-server-id');
            const targetServerId = this.getAttribute('data-server-id');

            // 确定插入位置
            const rect = this.getBoundingClientRect();
            const midpoint = rect.top + rect.height / 2;
            const insertBefore = e.clientY < midpoint;

            // 执行拖拽排序
            performServerDragSort(draggedServerId, targetServerId, insertBefore);
        });
    });
}

// 执行服务器拖拽排序
async function performServerDragSort(draggedServerId, targetServerId, insertBefore) {
    try {
        // 获取当前服务器列表的ID顺序
        const currentOrder = serverList.map(server => server.id);

        // 计算新的排序
        const draggedIndex = currentOrder.indexOf(draggedServerId);
        const targetIndex = currentOrder.indexOf(targetServerId);

        if (draggedIndex === -1 || targetIndex === -1) {
            throw new Error('无法找到服务器');
        }

        // 创建新的排序数组
        const newOrder = [...currentOrder];
        newOrder.splice(draggedIndex, 1); // 移除拖拽的元素

        // 计算插入位置
        let insertIndex = targetIndex;
        if (draggedIndex < targetIndex) {
            insertIndex = targetIndex - 1;
        }
        if (!insertBefore) {
            insertIndex += 1;
        }

        newOrder.splice(insertIndex, 0, draggedServerId); // 插入到新位置

        // 发送批量排序请求
        await apiRequest('/api/admin/servers/batch-reorder', {
            method: 'POST',
            body: JSON.stringify({ serverIds: newOrder })
        });

        // 重新加载服务器列表
        await loadServerList();
        showToast('success', '服务器排序已更新');

    } catch (error) {
                showToast('danger', '拖拽排序失败: ' + error.message);
        // 重新加载以恢复原始状态
        loadServerList();
    }
}


// Function to copy VPS installation script
async function copyVpsInstallScript(serverId, serverName, buttonElement) {
    const originalButtonHtml = buttonElement.innerHTML;
    buttonElement.disabled = true;
    buttonElement.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> 生成中...';

    try {
        // 获取包含完整API密钥的服务器信息
        const response = await apiRequest('/api/admin/servers?full_key=true');
        const server = response.servers.find(s => s.id === serverId);

        if (!server || !server.api_key) {
            throw new Error('未找到服务器或API密钥，请刷新页面重试');
        }

        const apiKey = server.api_key;
        const workerUrl = window.location.origin;

        // 使用GitHub上的脚本地址
        const baseScriptUrl = "https://raw.githubusercontent.com/kadidalax/cf-vps-monitor/main/cf-vps-monitor.sh";
        // 生成安装命令（让脚本自动从服务器获取上报间隔）
        const scriptCommand = 'wget ' + baseScriptUrl + ' -O cf-vps-monitor.sh && chmod +x cf-vps-monitor.sh && ./cf-vps-monitor.sh -i -k ' + apiKey + ' -s ' + serverId + ' -u ' + workerUrl;

        await navigator.clipboard.writeText(scriptCommand);

        buttonElement.innerHTML = '<i class="bi bi-check-lg"></i> 已复制!';
        buttonElement.classList.remove('btn-outline-info');
        buttonElement.classList.add('btn-success');

        showToast('success', '服务器 "' + serverName + '" 的安装脚本已复制到剪贴板');

    } catch (error) {
                showToast('danger', '复制脚本失败: ' + error.message);
        buttonElement.innerHTML = '<i class="bi bi-x-lg"></i> 复制失败';
        buttonElement.classList.remove('btn-outline-info');
        buttonElement.classList.add('btn-danger');
    } finally {
        setTimeout(() => {
            buttonElement.disabled = false;
            buttonElement.innerHTML = originalButtonHtml;
            buttonElement.classList.remove('btn-success', 'btn-danger');
            buttonElement.classList.add('btn-outline-info');
        }, 3000); // Revert button state after 3 seconds
    }
}

// 更新服务器显示状态
async function updateServerVisibility(serverId, isPublic, originalState, toggleElement) {
    const startTime = Date.now();
        try {
        const data = await apiRequest('/api/admin/servers/' + serverId + '/visibility', {
            method: 'POST',
            body: JSON.stringify({ is_public: isPublic })
        });

        const requestTime = Date.now() - startTime;
                // 更新本地数据
        const serverIndex = serverList.findIndex(s => s.id === serverId);
        if (serverIndex !== -1) {
            serverList[serverIndex].is_public = isPublic;
        }

        // 成功后设置最终正常状态 - 使用可靠的恢复机制
        function restoreButtonState(retryCount = 0) {
            const currentToggle = document.querySelector('.server-visibility-toggle[data-server-id="' + serverId + '"]');
            if (currentToggle) {
                                currentToggle.checked = isPublic;
                currentToggle.style.opacity = '1';
                currentToggle.disabled = false;
                delete currentToggle.dataset.updating;

                // 直接显示成功提醒
                showToast('success', '服务器显示状态已' + (isPublic ? '开启' : '关闭'));
            } else if (retryCount < 3) {
                                setTimeout(() => restoreButtonState(retryCount + 1), 100);
            } else {
                // 静默处理按钮元素未找到
            }
        }

        // 立即尝试恢复，如果失败则重试
        restoreButtonState();

    } catch (error) {
                // 失败时恢复原始状态
        const currentToggle = document.querySelector('.server-visibility-toggle[data-server-id="' + serverId + '"]');
        if (currentToggle) {
            currentToggle.checked = originalState;
            currentToggle.style.opacity = '1';
            currentToggle.disabled = false;
            delete currentToggle.dataset.updating;

            // 直接显示错误提醒，不需要等待状态变化
            showToast('danger', '更新显示状态失败: ' + error.message);
        } else {
            // 如果找不到开关元素，立即显示错误
            showToast('danger', '更新显示状态失败: ' + error.message);
        }
    }
}

// 移动服务器顺序
async function moveServer(serverId, direction) {
    try {
        await apiRequest('/api/admin/servers/' + serverId + '/reorder', {
            method: 'POST',
            body: JSON.stringify({ direction })
        });

        // 重新加载列表以反映新顺序
        await loadServerList();
        showToast('success', '服务器已成功' + (direction === 'up' ? '上移' : '下移'));

    } catch (error) {
                showToast('danger', '移动服务器失败: ' + error.message);
    }
}

// 显示服务器模态框（添加模式）
function showServerModal() {
    // 重置表单和标记
    document.getElementById('serverForm').reset();
    document.getElementById('serverId').value = '';
    document.getElementById('apiKeyGroup').classList.add('d-none');
    document.getElementById('serverIdDisplayGroup').classList.add('d-none');
    document.getElementById('workerUrlDisplayGroup').classList.add('d-none');
    hasAddedNewServer = false; // 重置新服务器标记

    // 设置模态框标题
    document.getElementById('serverModalTitle').textContent = '添加服务器';

    // 显示模态框
    const serverModal = new bootstrap.Modal(document.getElementById('serverModal'));
    serverModal.show();
}

// 编辑服务器
function editServer(serverId) {
    const server = serverList.find(s => s.id === serverId);
    if (!server) return;

    // 填充表单
    document.getElementById('serverId').value = server.id;
    document.getElementById('serverName').value = server.name;
    document.getElementById('serverDescription').value = server.description || '';
    document.getElementById('apiKeyGroup').classList.add('d-none');
    document.getElementById('serverIdDisplayGroup').classList.add('d-none');
    document.getElementById('workerUrlDisplayGroup').classList.add('d-none');

    // 设置模态框标题
    document.getElementById('serverModalTitle').textContent = '编辑服务器';

    // 显示模态框
    const serverModal = new bootstrap.Modal(document.getElementById('serverModal'));
    serverModal.show();
}

// 保存服务器
async function saveServer() {
    const serverId = document.getElementById('serverId').value;
    const serverName = document.getElementById('serverName').value.trim();
    const serverDescription = document.getElementById('serverDescription').value.trim();
    // const enableFrequentNotifications = document.getElementById('serverEnableFrequentNotifications').checked; // Removed

    if (!serverName) {
        showToast('warning', '服务器名称不能为空');
        return;
    }

    try {
        let data;

        if (serverId) {
            // 更新服务器
            data = await apiRequest('/api/admin/servers/' + serverId, {
                method: 'PUT',
                body: JSON.stringify({
                    name: serverName,
                    description: serverDescription
                })
            });
        } else {
            // 添加服务器
            data = await apiRequest('/api/admin/servers', {
                method: 'POST',
                body: JSON.stringify({
                    name: serverName,
                    description: serverDescription
                })
            });
        }

        // 如果是新添加的服务器，流畅地切换到密钥显示（不隐藏模态框）
        if (!serverId && data.server && data.server.api_key) {
            hasAddedNewServer = true; // 标记已添加新服务器

            // 直接在当前模态框中显示密钥信息，提供流畅的用户体验
            // 不隐藏模态框，而是切换内容，让用户感觉是自然的过渡
            showApiKeyInCurrentModal(data.server);
            showToast('success', '服务器添加成功');

            // 在后台异步刷新服务器列表
            loadServerList().catch(error => {
                            });
        } else {
            // 编辑服务器的情况，正常隐藏模态框并刷新列表
            const serverModal = bootstrap.Modal.getInstance(document.getElementById('serverModal'));
            serverModal.hide();

            await loadServerList();
            showToast('success', serverId ? '服务器更新成功' : '服务器添加成功');
        }
    } catch (error) {
                showToast('danger', '保存服务器失败，请稍后重试');
    }
}

// 查看API密钥（获取完整密钥版本）
async function viewApiKey(serverId) {
    try {
        // 请求包含完整API密钥的服务器信息
        const response = await apiRequest('/api/admin/servers?full_key=true');
        const server = response.servers.find(s => s.id === serverId);

        if (server && server.api_key) {
            showApiKey(server);
        } else {
            showToast('danger', '未找到服务器信息或API密钥，请稍后重试');
        }
    } catch (error) {
                showToast('danger', '查看API密钥失败，请稍后重试');
    }
}

// 在当前模态框中显示API密钥（用于添加服务器后的流畅过渡）
function showApiKeyInCurrentModal(server) {
    // 填充表单数据
    document.getElementById('serverId').value = server.id;
    document.getElementById('serverName').value = server.name;
    document.getElementById('serverDescription').value = server.description || '';

    // 显示API密钥、服务器ID和Worker URL
    document.getElementById('apiKey').value = server.api_key;
    document.getElementById('apiKeyGroup').classList.remove('d-none');

    document.getElementById('serverIdDisplay').value = server.id;
    document.getElementById('serverIdDisplayGroup').classList.remove('d-none');

    document.getElementById('workerUrlDisplay').value = window.location.origin;
    document.getElementById('workerUrlDisplayGroup').classList.remove('d-none');

    // 更新模态框标题
    document.getElementById('serverModalTitle').textContent = '服务器详细信息与密钥';

    // 注意：不创建新的模态框，而是在当前模态框中切换内容
    // 这样用户感觉是自然的内容过渡，而不是突然弹出新窗口
}

// 显示API密钥（用于查看密钥按钮）
function showApiKey(server) {
    // 填充表单
    document.getElementById('serverId').value = server.id; // Hidden input for form submission if needed
    document.getElementById('serverName').value = server.name;
    document.getElementById('serverDescription').value = server.description || '';

    // Populate and show API Key, Server ID, and Worker URL
    document.getElementById('apiKey').value = server.api_key;
    document.getElementById('apiKeyGroup').classList.remove('d-none');

    document.getElementById('serverIdDisplay').value = server.id;
    document.getElementById('serverIdDisplayGroup').classList.remove('d-none');

    document.getElementById('workerUrlDisplay').value = window.location.origin;
    document.getElementById('workerUrlDisplayGroup').classList.remove('d-none');

    // 设置模态框标题
    document.getElementById('serverModalTitle').textContent = '服务器详细信息与密钥';

    // 显示模态框
    const serverModal = new bootstrap.Modal(document.getElementById('serverModal'));
    serverModal.show();
}

// 显示删除确认
function showDeleteConfirmation(serverId, serverName) {
    currentServerId = serverId;
    document.getElementById('deleteServerName').textContent = serverName;

    const deleteModal = new bootstrap.Modal(document.getElementById('deleteModal'));
    deleteModal.show();
}

// 删除服务器
async function deleteServer(serverId) {
    try {
        await apiRequest('/api/admin/servers/' + serverId + '?confirm=true', {
            method: 'DELETE'
        });

        // 隐藏模态框
        const deleteModal = bootstrap.Modal.getInstance(document.getElementById('deleteModal'));
        deleteModal.hide();

        // 重新加载服务器列表
        loadServerList();
        showToast('success', '服务器删除成功');
    } catch (error) {
                showToast('danger', '删除服务器失败，请稍后重试');
    }
}


// --- Site Monitoring Functions (Continued) ---

// 更新网站显示状态
async function updateSiteVisibility(siteId, isPublic, originalState, toggleElement) {
    const startTime = Date.now();
        try {
        await apiRequest('/api/admin/sites/' + siteId + '/visibility', {
            method: 'POST',
            body: JSON.stringify({ is_public: isPublic })
        });

        const requestTime = Date.now() - startTime;
                        // 更新本地数据
        const siteIndex = siteList.findIndex(s => s.id === siteId);
        if (siteIndex !== -1) {
            siteList[siteIndex].is_public = isPublic;
        }

        // 成功后设置最终正常状态 - 使用可靠的恢复机制
        function restoreButtonState(retryCount = 0) {
            const currentToggle = document.querySelector('.site-visibility-toggle[data-site-id="' + siteId + '"]');
            if (currentToggle) {
                                currentToggle.checked = isPublic;
                currentToggle.style.opacity = '1';
                currentToggle.disabled = false;
                delete currentToggle.dataset.updating;

                // 直接显示成功提醒
                showToast('success', '网站显示状态已' + (isPublic ? '开启' : '关闭'));
            } else if (retryCount < 3) {
                                setTimeout(() => restoreButtonState(retryCount + 1), 100);
            } else {
                // 静默处理网站按钮元素未找到
            }
        }

        // 立即尝试恢复，如果失败则重试
        restoreButtonState();

    } catch (error) {
                // 失败时恢复原始状态
        const currentToggle = document.querySelector('.site-visibility-toggle[data-site-id="' + siteId + '"]');
        if (currentToggle) {
            currentToggle.checked = originalState;
            currentToggle.style.opacity = '1';
            currentToggle.disabled = false;
            delete currentToggle.dataset.updating;

            // 直接显示错误提醒，不需要等待状态变化
            showToast('danger', '更新显示状态失败: ' + error.message);
        } else {
            // 如果找不到开关元素，立即显示错误
            showToast('danger', '更新显示状态失败: ' + error.message);
        }
    }
}

// 移动网站顺序
async function moveSite(siteId, direction) {
    try {
        await apiRequest('/api/admin/sites/' + siteId + '/reorder', {
            method: 'POST',
            body: JSON.stringify({ direction })
        });

        // 重新加载列表以反映新顺序
        await loadSiteList();
        showToast('success', '网站已成功' + (direction === 'up' ? '上移' : '下移'));

    } catch (error) {
                showToast('danger', '移动网站失败: ' + error.message);
    }
}


// --- Password Management Functions ---

// 显示密码修改模态框
function showPasswordModal() {
    // 重置表单
    document.getElementById('passwordForm').reset();

    const passwordModal = new bootstrap.Modal(document.getElementById('passwordModal'));
    passwordModal.show();
}

// 修改密码
async function changePassword() {
    const currentPassword = document.getElementById('currentPassword').value;
    const newPassword = document.getElementById('newPassword').value;
    const confirmPassword = document.getElementById('confirmPassword').value;

    // 验证输入
    if (!currentPassword || !newPassword || !confirmPassword) {
        showToast('warning', '所有密码字段都必须填写');
        return;
    }

    if (newPassword !== confirmPassword) {
        showToast('warning', '新密码和确认密码不匹配');
        return;
    }

    try {
        await apiRequest('/api/auth/change-password', {
            method: 'POST',
            body: JSON.stringify({
                current_password: currentPassword,
                new_password: newPassword
            })
        });

        // 隐藏模态框
        const passwordModal = bootstrap.Modal.getInstance(document.getElementById('passwordModal'));
        passwordModal.hide();

        // 清除默认密码提醒标记，这样如果用户再次使用默认密码登录会重新提醒
        localStorage.removeItem('hasShownDefaultPasswordWarning');

        showToast('success', '密码修改成功');
    } catch (error) {
                showToast('danger', '密码修改请求失败，请稍后重试');
    }
}


// --- Auth Functions ---

// 退出登录
function logout() {
    // 清除localStorage中的token和提醒标记
    localStorage.removeItem('auth_token');
    localStorage.removeItem('hasShownDefaultPasswordWarning');

    // 重定向到登录页面
    window.location.href = 'login.html';
}


// --- Site Monitoring Functions ---

// 加载监控网站列表
async function loadSiteList() {
    try {
        const data = await apiRequest('/api/admin/sites');
        siteList = data.sites || [];

        // 简化逻辑：直接渲染，智能状态显示会处理更新中的按钮
        renderSiteTable(siteList);
    } catch (error) {
                showToast('danger', '加载监控网站列表失败: ' + error.message);
    }
}

// 渲染监控网站表格
function renderSiteTable(sites) {
    const tableBody = document.getElementById('siteTableBody');

    // 简化状态管理：不再需要复杂的状态保存机制

    tableBody.innerHTML = '';

    if (sites.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="9" class="text-center">暂无监控网站</td></tr>'; // Colspan updated
        // 同时更新移动端卡片
        renderMobileAdminSiteCards([]);
        return;
    }

    sites.forEach((site, index) => { // Added index for sorting buttons
        const row = document.createElement('tr');
        row.setAttribute('data-site-id', site.id);
        row.classList.add('site-row-draggable');
        row.draggable = true;

        const statusInfo = getSiteStatusBadge(site.last_status);
        const lastCheckTime = site.last_checked ? new Date(site.last_checked * 1000).toLocaleString() : '从未';
        const responseTime = site.last_response_time_ms !== null ? \`\${site.last_response_time_ms} ms\` : '-';

        // 智能状态显示：完整保存更新中按钮的所有状态
        const existingToggle = document.querySelector('.site-visibility-toggle[data-site-id="' + site.id + '"]');
        const isCurrentlyUpdating = existingToggle && existingToggle.dataset.updating === 'true';
        const displayState = isCurrentlyUpdating ? existingToggle.checked : site.is_public;
        const needsUpdatingState = isCurrentlyUpdating;

        row.innerHTML = \`
             <td>
                <div class="btn-group btn-group-sm">
                    <i class="bi bi-grip-vertical text-muted me-2" style="cursor: grab;" title="拖拽排序"></i>
                     <button class="btn btn-outline-secondary move-site-btn" data-id="\${site.id}" data-direction="up" \${index === 0 ? 'disabled' : ''} title="上移">
                        <i class="bi bi-arrow-up"></i>
                    </button>
                     <button class="btn btn-outline-secondary move-site-btn" data-id="\${site.id}" data-direction="down" \${index === sites.length - 1 ? 'disabled' : ''} title="下移">
                        <i class="bi bi-arrow-down"></i>
                    </button>
                </div>
            </td>
            <td>\${site.name || '-'}</td>
            <td><a href="\${site.url}" target="_blank" rel="noopener noreferrer">\${site.url}</a></td>
            <td><span class="badge \${statusInfo.class}">\${statusInfo.text}</span></td>
            <td>\${site.last_status_code || '-'}</td>
            <td>\${responseTime}</td>
            <td>\${lastCheckTime}</td>
            <td>
                <div class="form-check form-switch">
                    <input class="form-check-input site-visibility-toggle" type="checkbox" data-site-id="\${site.id}" \${displayState ? 'checked' : ''}\${needsUpdatingState ? ' data-updating="true"' : ''}>
                </div>
            </td>
            <td>
                <div class="btn-group">
                    <button class="btn btn-sm btn-outline-primary edit-site-btn" data-id="\${site.id}" title="编辑">
                        <i class="bi bi-pencil"></i>
                    </button>
                    <button class="btn btn-sm btn-outline-danger delete-site-btn" data-id="\${site.id}" data-name="\${site.name || site.url}" data-url="\${site.url}" title="删除">
                        <i class="bi bi-trash"></i>
                    </button>
                </div>
            </td>
        \`;
        tableBody.appendChild(row);
    });

    // 初始化拖拽排序
    initializeSiteDragSort();

    // Add event listeners for edit and delete buttons
    document.querySelectorAll('.edit-site-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const siteId = this.getAttribute('data-id');
            editSite(siteId);
        });
    });

    document.querySelectorAll('.delete-site-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const siteId = this.getAttribute('data-id');
            const siteName = this.getAttribute('data-name');
            const siteUrl = this.getAttribute('data-url');
            showDeleteSiteConfirmation(siteId, siteName, siteUrl);
        });
    });

    // Add event listeners for move buttons
    document.querySelectorAll('.move-site-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const siteId = this.getAttribute('data-id');
            const direction = this.getAttribute('data-direction');
            moveSite(siteId, direction);
        });
    });

    // 优化的网站显示开关事件监听 - 直接处理状态切换
    document.querySelectorAll('.site-visibility-toggle').forEach(toggle => {
        toggle.addEventListener('click', function(event) {
            // 如果开关正在更新中，忽略点击
            if (this.disabled || this.dataset.updating === 'true') {
                event.preventDefault();
                return;
            }

            const siteId = this.getAttribute('data-site-id');
            const targetState = this.checked; // 点击后的状态就是目标状态
            const originalState = !this.checked; // 原始状态是目标状态的相反

                        // 立即设置为加载状态
            this.disabled = true;
            this.style.opacity = '0.6';
            this.dataset.updating = 'true';

            updateSiteVisibility(siteId, targetState, originalState, this);
        });
    });

    // 重新应用正在更新按钮的视觉状态（因为重新渲染会创建新元素）
    document.querySelectorAll('.site-visibility-toggle[data-updating="true"]').forEach(toggle => {
        toggle.disabled = true;
        toggle.style.opacity = '0.6';
    });

    // 同时渲染移动端卡片
    renderMobileAdminSiteCards(sites);
}

// 初始化网站拖拽排序
function initializeSiteDragSort() {
    const tableBody = document.getElementById('siteTableBody');
    if (!tableBody) return;

    let draggedElement = null;
    let draggedOverElement = null;

    // 为所有可拖拽行添加事件监听
    const draggableRows = tableBody.querySelectorAll('.site-row-draggable');

    draggableRows.forEach(row => {
        row.addEventListener('dragstart', function(e) {
            draggedElement = this;
            this.style.opacity = '0.5';
            e.dataTransfer.effectAllowed = 'move';
            e.dataTransfer.setData('text/html', this.outerHTML);
        });

        row.addEventListener('dragend', function(e) {
            this.style.opacity = '';
            draggedElement = null;
            draggedOverElement = null;

            // 移除所有拖拽样式
            draggableRows.forEach(r => {
                r.classList.remove('drag-over-top', 'drag-over-bottom');
            });
        });

        row.addEventListener('dragover', function(e) {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';

            if (this === draggedElement) return;

            draggedOverElement = this;

            // 移除其他行的拖拽样式
            draggableRows.forEach(r => {
                if (r !== this) {
                    r.classList.remove('drag-over-top', 'drag-over-bottom');
                }
            });

            // 确定插入位置
            const rect = this.getBoundingClientRect();
            const midpoint = rect.top + rect.height / 2;

            if (e.clientY < midpoint) {
                this.classList.add('drag-over-top');
                this.classList.remove('drag-over-bottom');
            } else {
                this.classList.add('drag-over-bottom');
                this.classList.remove('drag-over-top');
            }
        });

        row.addEventListener('drop', function(e) {
            e.preventDefault();

            if (this === draggedElement) return;

            const draggedSiteId = draggedElement.getAttribute('data-site-id');
            const targetSiteId = this.getAttribute('data-site-id');

            // 确定插入位置
            const rect = this.getBoundingClientRect();
            const midpoint = rect.top + rect.height / 2;
            const insertBefore = e.clientY < midpoint;

            // 执行拖拽排序
            performSiteDragSort(draggedSiteId, targetSiteId, insertBefore);
        });
    });
}

// 执行网站拖拽排序
async function performSiteDragSort(draggedSiteId, targetSiteId, insertBefore) {
    try {
        // 获取当前网站列表的ID顺序
        const currentOrder = siteList.map(site => site.id);

        // 计算新的排序
        const draggedIndex = currentOrder.indexOf(draggedSiteId);
        const targetIndex = currentOrder.indexOf(targetSiteId);

        if (draggedIndex === -1 || targetIndex === -1) {
            throw new Error('无法找到网站');
        }

        // 创建新的排序数组
        const newOrder = [...currentOrder];
        newOrder.splice(draggedIndex, 1); // 移除拖拽的元素

        // 计算插入位置
        let insertIndex = targetIndex;
        if (draggedIndex < targetIndex) {
            insertIndex = targetIndex - 1;
        }
        if (!insertBefore) {
            insertIndex += 1;
        }

        newOrder.splice(insertIndex, 0, draggedSiteId); // 插入到新位置

        // 发送批量排序请求
        await apiRequest('/api/admin/sites/batch-reorder', {
            method: 'POST',
            body: JSON.stringify({ siteIds: newOrder })
        });

        // 重新加载网站列表
        await loadSiteList();
        showToast('success', '网站排序已更新');

    } catch (error) {
                showToast('danger', '拖拽排序失败: ' + error.message);
        // 重新加载以恢复原始状态
        loadSiteList();
    }
}

// 获取网站状态对应的Badge样式和文本
function getSiteStatusBadge(status) {
    switch (status) {
        case 'UP': return { class: 'bg-success', text: '正常' };
        case 'HIGH_LATENCY': return { class: 'bg-warning text-dark', text: '高延迟' };
        case 'DOWN': return { class: 'bg-danger', text: '故障' };
        case 'TIMEOUT': return { class: 'bg-warning text-dark', text: '超时' };
        case 'ERROR': return { class: 'bg-danger', text: '错误' };
        case 'PENDING': return { class: 'bg-secondary', text: '待检测' };
        default: return { class: 'bg-secondary', text: '未知' };
    }
}


// 显示添加/编辑网站模态框 (handles both add and edit)
function showSiteModal(siteIdToEdit = null) {
    const form = document.getElementById('siteForm');
    form.reset();
    const modalTitle = document.getElementById('siteModalTitle');
    const siteIdInput = document.getElementById('siteId');

    if (siteIdToEdit) {
        const site = siteList.find(s => s.id === siteIdToEdit);
        if (site) {
            modalTitle.textContent = '编辑监控网站';
            siteIdInput.value = site.id;
            document.getElementById('siteName').value = site.name || '';
            document.getElementById('siteUrl').value = site.url;
            // document.getElementById('siteEnableFrequentNotifications').checked = site.enable_frequent_down_notifications || false; // Removed
        } else {
            showToast('danger', '未找到要编辑的网站信息');
            return;
        }
    } else {
        modalTitle.textContent = '添加监控网站';
        siteIdInput.value = ''; // Clear ID for add mode
        // document.getElementById('siteEnableFrequentNotifications').checked = false; // Removed
    }

    const siteModal = new bootstrap.Modal(document.getElementById('siteModal'));
    siteModal.show();
}

// Function to call when edit button is clicked
function editSite(siteId) {
    showSiteModal(siteId);
}

// 保存网站（添加或更新）
async function saveSite() {
    const siteId = document.getElementById('siteId').value; // Get ID from hidden input
    const siteName = document.getElementById('siteName').value.trim();
    const siteUrl = document.getElementById('siteUrl').value.trim();
    // const enableFrequentNotifications = document.getElementById('siteEnableFrequentNotifications').checked; // Removed

    if (!siteUrl) {
        showToast('warning', '请输入网站URL');
        return;
    }
    if (!siteUrl.startsWith('http://') && !siteUrl.startsWith('https://')) {
         showToast('warning', 'URL必须以 http:// 或 https:// 开头');
         return;
    }

    const requestBody = {
        url: siteUrl,
        name: siteName
        // enable_frequent_down_notifications: enableFrequentNotifications // Removed
    };
    let apiUrl = '/api/admin/sites';
    let method = 'POST';

    if (siteId) { // If siteId exists, it's an update
        apiUrl = \`/api/admin/sites/\${siteId}\`;
        method = 'PUT';
    }

    try {
        const responseData = await apiRequest(apiUrl, {
            method: method,
            body: JSON.stringify(requestBody)
        });

        const siteModalInstance = bootstrap.Modal.getInstance(document.getElementById('siteModal'));
        if (siteModalInstance) {
            siteModalInstance.hide();
        }

        await loadSiteList(); // Reload the list
        showToast('success', '监控网站' + (siteId ? '更新' : '添加') + '成功');

    } catch (error) {
                showToast('danger', '保存网站失败: ' + error.message);
    }
}

// 显示删除网站确认模态框
function showDeleteSiteConfirmation(siteId, siteName, siteUrl) {
    currentSiteId = siteId;
    document.getElementById('deleteSiteName').textContent = siteName;
    document.getElementById('deleteSiteUrl').textContent = siteUrl;
    const deleteModal = new bootstrap.Modal(document.getElementById('deleteSiteModal'));
    deleteModal.show();
}


// 删除网站监控
async function deleteSite(siteId) {
    try {
        await apiRequest(\`/api/admin/sites/\${siteId}?confirm=true\`, {
            method: 'DELETE'
        });

        // Hide modal and reload list
        const deleteModal = bootstrap.Modal.getInstance(document.getElementById('deleteSiteModal'));
        deleteModal.hide();
        await loadSiteList(); // Reload list
        showToast('success', '网站监控已删除');
        currentSiteId = null; // Reset current ID

    } catch (error) {
                showToast('danger', '删除网站失败: ' + error.message);
    }
}


// ==================== LLM端点管理函数 ====================

// 加载LLM端点列表
async function loadLlmEndpointList() {
    try {
        const data = await apiRequest('/api/admin/llm-endpoints');
        llmEndpointList = data.endpoints || [];
        renderGroupedAdminLlmTable(llmEndpointList);
        renderGroupedMobileAdminLlmCards(llmEndpointList);
    } catch (error) {
        showToast('danger', '加载LLM端点列表失败: ' + error.message);
    }
}

// 渲染LLM端点管理表格
function renderLlmEndpointTable(endpoints) {
    const tableBody = document.getElementById('llmEndpointTableBody');
    if (!tableBody) return;

    tableBody.innerHTML = '';

    if (endpoints.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="10" class="text-center">暂无LLM端点</td></tr>';
        return;
    }

    endpoints.forEach((ep) => {
        const row = document.createElement('tr');
        row.setAttribute('data-llm-id', ep.id);
        row.classList.add('llm-row-draggable');
        row.draggable = true;

        const statusInfo = getSiteStatusBadge(ep.last_status);
        const lastCheckTime = ep.last_checked ? new Date(ep.last_checked * 1000).toLocaleString() : '从未';
        const responseTime = ep.last_response_time_ms !== null ? \`\${ep.last_response_time_ms} ms\` : '-';

        row.innerHTML = \`
            <td><i class="bi bi-grip-vertical text-muted me-2" style="cursor: grab;" title="拖拽排序"></i></td>
            <td><span class="badge bg-info">\${ep.model}</span></td>
            <td><a href="\${ep.api_url}" target="_blank" rel="noopener noreferrer" class="text-truncate d-inline-block" style="max-width:200px;">\${ep.api_url}</a></td>
            <td><span class="badge \${statusInfo.class}">\${statusInfo.text}</span></td>
            <td>\${ep.last_status_code || '-'}</td>
            <td>\${responseTime}</td>
            <td>\${lastCheckTime}</td>
            <td>
                <div class="form-check form-switch">
                    <input class="form-check-input llm-visibility-toggle" type="checkbox" data-llm-id="\${ep.id}" \${ep.is_public ? 'checked' : ''}>
                </div>
            </td>
            <td>
                <div class="form-check form-switch">
                    <input class="form-check-input llm-notification-toggle" type="checkbox" data-llm-id="\${ep.id}" \${ep.enable_notifications !== 0 ? 'checked' : ''}>
                </div>
            </td>
            <td>
                <div class="btn-group">
                    <button class="btn btn-sm btn-outline-primary edit-llm-btn" data-id="\${ep.id}" title="编辑">
                        <i class="bi bi-pencil"></i>
                    </button>
                    <button class="btn btn-sm btn-outline-danger delete-llm-btn" data-id="\${ep.id}" data-name="\${ep.model}" title="删除">
                        <i class="bi bi-trash"></i>
                    </button>
                </div>
            </td>
        \`;
        tableBody.appendChild(row);
    });

    // 桌面端事件 - 限域到 tableBody
    tableBody.querySelectorAll('.edit-llm-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            showLlmEndpointModal(this.getAttribute('data-id'));
        });
    });

    tableBody.querySelectorAll('.delete-llm-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            showDeleteLlmEndpointConfirmation(this.getAttribute('data-id'), this.getAttribute('data-name'));
        });
    });

    tableBody.querySelectorAll('.llm-visibility-toggle').forEach(toggle => {
        toggle.addEventListener('change', async function() {
            const endpointId = this.getAttribute('data-llm-id');
            const isPublic = this.checked;
            try {
                await apiRequest(\`/api/admin/llm-endpoints/\${endpointId}/visibility\`, {
                    method: 'PUT',
                    body: JSON.stringify({ is_public: isPublic })
                });
                showToast('success', '可见性更新成功');
            } catch (error) {
                this.checked = !this.checked;
                showToast('danger', '更新可见性失败: ' + error.message);
            }
        });
    });

    tableBody.querySelectorAll('.llm-notification-toggle').forEach(toggle => {
        toggle.addEventListener('change', async function() {
            const endpointId = this.getAttribute('data-llm-id');
            const enableNotifications = this.checked;
            try {
                await apiRequest(\`/api/admin/llm-endpoints/\${endpointId}/notifications\`, {
                    method: 'PUT',
                    body: JSON.stringify({ enable_notifications: enableNotifications })
                });
                showToast('success', '通知设置更新成功');
            } catch (error) {
                this.checked = !this.checked;
                showToast('danger', '更新通知设置失败: ' + error.message);
            }
        });
    });

    // 初始化拖拽排序
    initializeLlmDragSort();
}

// 初始化LLM端点拖拽排序
function initializeLlmDragSort() {
    const tableBody = document.getElementById('llmEndpointTableBody');
    if (!tableBody) return;

    let draggedElement = null;
    let draggedOverElement = null;

    // 为所有可拖拽行添加事件监听
    const draggableRows = tableBody.querySelectorAll('.llm-row-draggable');

    draggableRows.forEach(row => {
        row.addEventListener('dragstart', function(e) {
            draggedElement = this;
            this.style.opacity = '0.5';
            e.dataTransfer.effectAllowed = 'move';
            e.dataTransfer.setData('text/html', this.outerHTML);
        });

        row.addEventListener('dragend', function(e) {
            this.style.opacity = '';
            draggedElement = null;
            draggedOverElement = null;

            // 移除所有拖拽样式
            draggableRows.forEach(r => {
                r.classList.remove('drag-over-top', 'drag-over-bottom');
            });
        });

        row.addEventListener('dragover', function(e) {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';

            if (this === draggedElement) return;

            draggedOverElement = this;

            // 移除其他行的拖拽样式
            draggableRows.forEach(r => {
                if (r !== this) {
                    r.classList.remove('drag-over-top', 'drag-over-bottom');
                }
            });

            // 确定插入位置
            const rect = this.getBoundingClientRect();
            const midpoint = rect.top + rect.height / 2;

            if (e.clientY < midpoint) {
                this.classList.add('drag-over-top');
                this.classList.remove('drag-over-bottom');
            } else {
                this.classList.add('drag-over-bottom');
                this.classList.remove('drag-over-top');
            }
        });

        row.addEventListener('drop', function(e) {
            e.preventDefault();

            if (this === draggedElement) return;

            const draggedEndpointId = draggedElement.getAttribute('data-llm-id');
            const targetEndpointId = this.getAttribute('data-llm-id');

            // 确定插入位置
            const rect = this.getBoundingClientRect();
            const midpoint = rect.top + rect.height / 2;
            const insertBefore = e.clientY < midpoint;

            // 执行拖拽排序
            performLlmDragSort(draggedEndpointId, targetEndpointId, insertBefore);
        });
    });
}

// 执行LLM端点拖拽排序
async function performLlmDragSort(draggedEndpointId, targetEndpointId, insertBefore) {
    try {
        // 获取当前LLM端点列表的ID顺序
        const currentOrder = llmEndpointList.map(ep => ep.id);

        // 计算新的排序
        const draggedIndex = currentOrder.indexOf(draggedEndpointId);
        const targetIndex = currentOrder.indexOf(targetEndpointId);

        if (draggedIndex === -1 || targetIndex === -1) {
            throw new Error('无法找到LLM端点');
        }

        // 创建新的排序数组
        const newOrder = [...currentOrder];
        newOrder.splice(draggedIndex, 1); // 移除拖拽的元素

        // 计算插入位置
        let insertIndex = targetIndex;
        if (draggedIndex < targetIndex) {
            insertIndex = targetIndex - 1;
        }
        if (!insertBefore) {
            insertIndex += 1;
        }

        newOrder.splice(insertIndex, 0, draggedEndpointId); // 插入到新位置

        // 发送批量排序请求
        await apiRequest('/api/admin/llm-endpoints/batch-reorder', {
            method: 'POST',
            body: JSON.stringify({ endpointIds: newOrder })
        });

        // 重新加载LLM端点列表
        await loadLlmEndpointList();
        showToast('success', 'LLM端点排序已更新');

    } catch (error) {
                showToast('danger', '拖拽排序失败: ' + error.message);
        // 重新加载以恢复原始状态
        loadLlmEndpointList();
    }
}

// 显示添加/编辑LLM端点模态框
function showLlmEndpointModal(endpointIdToEdit = null) {
    const form = document.getElementById('llmEndpointForm');
    form.reset();
    const modalTitle = document.getElementById('llmEndpointModalTitle');
    const endpointIdInput = document.getElementById('llmEndpointId');

    if (endpointIdToEdit) {
        const ep = llmEndpointList.find(e => e.id === endpointIdToEdit);
        if (ep) {
            modalTitle.textContent = '编辑LLM端点';
            endpointIdInput.value = ep.id;
            document.getElementById('llmEndpointProviderName').value = ep.name || '';
            document.getElementById('llmEndpointUrl').value = ep.api_url;
            document.getElementById('llmEndpointApiKey').value = ep.api_key || '';
            document.getElementById('llmEndpointModel').value = ep.model;
            document.getElementById('llmEndpointPrompt').value = ep.check_prompt || '';
            document.getElementById('llmEndpointExpected').value = ep.expected_contains || '';
            document.getElementById('llmEndpointTimeout').value = ep.timeout_ms || 45000;
        } else {
            showToast('danger', '未找到要编辑的端点信息');
            return;
        }
    } else {
        modalTitle.textContent = '添加LLM端点';
        endpointIdInput.value = '';
    }

    updateLlmTimeoutHelp();

    const modal = new bootstrap.Modal(document.getElementById('llmEndpointModal'));
    modal.show();
}

// 复用模型解析函数：支持 JSON 数组、换行分隔、逗号分隔，去重
function parseLlmModelInput(input) {
    if (!input || !input.trim()) return [];
    const trimmed = input.trim();

    // 尝试 JSON 数组解析
    if (trimmed.startsWith('[')) {
        try {
            const parsed = JSON.parse(trimmed);
            if (!Array.isArray(parsed)) return [];
            const models = parsed.map(item => {
                if (typeof item === 'string') return item.trim();
                if (item && typeof item === 'object' && item.model) return item.model.trim();
                return '';
            }).filter(Boolean);
            return [...new Set(models)];
        } catch (e) {
            // JSON 解析失败，降级到文本解析
        }
    }

    // 非 JSON：按换行或逗号分隔
    const parts = trimmed.split(/[\\n,]+/);
    const models = parts.map(p => p.trim()).filter(Boolean);
    return [...new Set(models)];
}

// 保存LLM端点（添加或更新）
async function saveLlmEndpoint() {
    const endpointId = document.getElementById('llmEndpointId').value;
    const name = document.getElementById('llmEndpointProviderName').value.trim();
    const api_url = document.getElementById('llmEndpointUrl').value.trim();
    const api_key = document.getElementById('llmEndpointApiKey').value.trim();
    const modelInput = document.getElementById('llmEndpointModel').value;
    const check_prompt = document.getElementById('llmEndpointPrompt').value.trim();
    const expected_contains = document.getElementById('llmEndpointExpected').value.trim();
    const timeout_ms = parseInt(document.getElementById('llmEndpointTimeout').value, 10) || 45000;

    if (!api_url) {
        showToast('warning', '请输入API URL');
        return;
    }
    if (!api_url.startsWith('http://') && !api_url.startsWith('https://')) {
        showToast('warning', 'URL必须以 http:// 或 https:// 开头');
        return;
    }

    const models = parseLlmModelInput(modelInput);
    if (models.length === 0) {
        showToast('warning', '请输入至少一个模型名称');
        return;
    }

    const llmThreshold = parseInt(document.getElementById('llmHighLatencyMs')?.value || '20000', 10) || 20000;
    const minTimeout = Math.max(21000, llmThreshold + 1000);
    if (timeout_ms < minTimeout || timeout_ms > 120000) {
        showToast('warning', '超时时间必须在 ' + minTimeout + ' 到 120000 ms 之间');
        return;
    }

    const requestBody = {
        name,
        api_url, api_key,
        model: models[0],
        models,
        check_prompt, expected_contains, timeout_ms
    };
    let apiUrl = '/api/admin/llm-endpoints';
    let method = 'POST';

    if (endpointId) {
        apiUrl = \`/api/admin/llm-endpoints/\${endpointId}\`;
        method = 'PUT';
    }

    try {
        const result = await apiRequest(apiUrl, { method, body: JSON.stringify(requestBody) });

        const modalInstance = bootstrap.Modal.getInstance(document.getElementById('llmEndpointModal'));
        if (modalInstance) modalInstance.hide();

        await loadLlmEndpointList();

        if (endpointId) {
            // 编辑模式
            const createdCount = result?.count || 0;
            if (createdCount > 0) {
                showToast('success', \`更新成功，并新增 \${createdCount} 个模型\`);
            } else {
                showToast('success', 'LLM端点更新成功');
            }
        } else {
            // 新增模式
            const totalCount = result?.count || models.length;
            showToast('success', \`添加 \${totalCount} 个LLM端点成功\`);
        }
    } catch (error) {
        showToast('danger', '保存LLM端点失败: ' + error.message);
    }
}

// 显示删除LLM端点确认模态框
function showDeleteLlmEndpointConfirmation(endpointId, endpointName) {
    currentLlmEndpointId = endpointId;
    document.getElementById('deleteLlmEndpointName').textContent = endpointName;
    const deleteModal = new bootstrap.Modal(document.getElementById('deleteLlmEndpointModal'));
    deleteModal.show();
}

// 删除LLM端点
async function deleteLlmEndpoint(endpointId) {
    try {
        await apiRequest(\`/api/admin/llm-endpoints/\${endpointId}?confirm=true\`, {
            method: 'DELETE'
        });

        const deleteModal = bootstrap.Modal.getInstance(document.getElementById('deleteLlmEndpointModal'));
        deleteModal.hide();
        await loadLlmEndpointList();
        showToast('success', 'LLM端点已删除');
        currentLlmEndpointId = null;
    } catch (error) {
        showToast('danger', '删除LLM端点失败: ' + error.message);
    }
}




// ==================== LLM检查频率设置 ====================

async function loadLlmCheckInterval() {
    try {
        const data = await apiRequest('/api/admin/settings/llm-check-interval');
        const input = document.getElementById('llmCheckInterval');
        if (input && data?.interval) {
            input.value = data.interval;
        }
    } catch (error) {
        // 静默处理
    }
}

async function saveLlmCheckInterval() {
    const input = document.getElementById('llmCheckInterval');
    const interval = parseInt(input.value, 10);

    if (!interval || interval < 1) {
        showToast('warning', '请输入大于0的整数');
        return;
    }

    try {
        await apiRequest('/api/admin/settings/llm-check-interval', {
            method: 'POST',
            body: JSON.stringify({ interval })
        });
        showToast('success', 'LLM探测频率已设为每 ' + interval + ' 次Cron执行检查一次');
    } catch (error) {
        showToast('danger', '保存失败: ' + error.message);
    }
}

async function loadHighLatencyThresholds() {
    try {
        const data = await apiRequest('/api/admin/settings/high-latency-thresholds');
        const websiteInput = document.getElementById('websiteHighLatencyMs');
        const llmInput = document.getElementById('llmHighLatencyMs');

        if (websiteInput && data?.websiteHighLatencyMs) {
            websiteInput.value = data.websiteHighLatencyMs;
        }
        if (llmInput && data?.llmHighLatencyMs) {
            llmInput.value = data.llmHighLatencyMs;
        }

        updateLlmTimeoutHelp(data?.llmHighLatencyMs);
    } catch (error) {
        updateLlmTimeoutHelp();
    }
}

function updateLlmTimeoutHelp(llmThresholdMs) {
    const llmInput = document.getElementById('llmHighLatencyMs');
    const timeoutInput = document.getElementById('llmEndpointTimeout');
    const timeoutHelp = document.getElementById('llmEndpointTimeoutHelp');
    const threshold = parseInt(llmThresholdMs || llmInput?.value || '20000', 10) || 20000;
    const minTimeout = Math.max(21000, threshold + 1000);

    if (timeoutInput) {
        timeoutInput.min = String(minTimeout);
        if (!timeoutInput.value || parseInt(timeoutInput.value, 10) < minTimeout) {
            timeoutInput.value = String(Math.max(45000, minTimeout));
        }
    }

    if (timeoutHelp) {
        timeoutHelp.textContent = '超时时间必须大于当前 LLM 高延迟阈值（' + threshold + ' ms），建议至少高 1000 ms。超过该阈值但请求成功时会标记为“高延迟”状态（仅展示）。';
    }
}

async function saveHighLatencyThresholds() {
    const websiteHighLatencyMs = parseInt(document.getElementById('websiteHighLatencyMs').value, 10);
    const llmHighLatencyMs = parseInt(document.getElementById('llmHighLatencyMs').value, 10);

    if (!websiteHighLatencyMs || !llmHighLatencyMs || websiteHighLatencyMs < 1 || llmHighLatencyMs < 1) {
        showToast('warning', '请输入大于 0 的整数阈值');
        return;
    }
    if (websiteHighLatencyMs >= 15000) {
        showToast('warning', '网站高延迟阈值必须小于 15000 ms');
        return;
    }
    if (llmHighLatencyMs >= 120000) {
        showToast('warning', 'LLM 高延迟阈值必须小于 120000 ms');
        return;
    }

    try {
        const result = await apiRequest('/api/admin/settings/high-latency-thresholds', {
            method: 'POST',
            body: JSON.stringify({ websiteHighLatencyMs, llmHighLatencyMs })
        });

        updateLlmTimeoutHelp(result?.llmHighLatencyMs || llmHighLatencyMs);
        showToast('success', '高延迟阈值已更新');
    } catch (error) {
        showToast('danger', '保存高延迟阈值失败: ' + error.message);
    }
}


// --- Utility Functions ---

// 统一Toast弹窗函数 (增强版)
function showToast(type, message, options = {}) {
    const defaults = {
        success: 3000,
        info: 5000,
        warning: 8000,
        danger: 10000
    };

    const duration = options.duration || defaults[type] || 5000;
    const persistent = options.persistent || false;

    const container = document.getElementById('toastContainer');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = 'unified-toast ' + type;

    const icons = {
        success: 'bi-check-circle-fill',
        danger: 'bi-x-circle-fill',
        warning: 'bi-exclamation-triangle-fill',
        info: 'bi-info-circle-fill'
    };

    toast.innerHTML =
        '<i class="toast-icon bi ' + icons[type] + '"></i>' +
        '<div class="toast-content">' + message + '</div>' +
        '<button class="toast-close" onclick="hideToast(this.parentElement)">×</button>' +
        (persistent ? '' : '<div class="toast-progress" style="animation-duration: ' + duration + 'ms"></div>');

    container.appendChild(toast);

    if (!persistent) {
        setTimeout(() => hideToast(toast), duration);
    }

    return toast;
}

function hideToast(toast) {
    if (!toast || toast.classList.contains('hiding')) return;
    toast.classList.add('hiding');
    setTimeout(function() {
        if (toast.parentNode) {
            toast.parentNode.removeChild(toast);
        }
    }, 300);
}





// --- Telegram Settings Functions ---

// 加载Telegram通知设置
async function loadTelegramSettings() {
    try {
        const settings = await apiRequest('/api/admin/telegram-settings');
        if (settings) {
            document.getElementById('telegramBotToken').value = settings.bot_token || '';
            document.getElementById('telegramChatId').value = settings.chat_id || '';
            document.getElementById('enableTelegramNotifications').checked = !!settings.enable_notifications;
        }
    } catch (error) {
                showToast('danger', '加载Telegram设置失败: ' + error.message);
    }
}

// 保存Telegram通知设置
async function saveTelegramSettings() {
    const botToken = document.getElementById('telegramBotToken').value.trim();
    const chatId = document.getElementById('telegramChatId').value.trim();
    let enableNotifications = document.getElementById('enableTelegramNotifications').checked;

    // If Bot Token or Chat ID is empty, automatically disable notifications
    if (!botToken || !chatId) {
        enableNotifications = false;
        document.getElementById('enableTelegramNotifications').checked = false; // Update the checkbox UI
        if (document.getElementById('enableTelegramNotifications').checked && (botToken || chatId)) { // Only show warning if user intended to enable
             showToast('warning', 'Bot Token 和 Chat ID 均不能为空才能启用通知。通知已自动禁用');
        }
    } else if (enableNotifications && (!botToken || !chatId)) { // This case should ideally not be hit due to above logic, but kept for safety
        showToast('warning', '启用通知时，Bot Token 和 Chat ID 不能为空');
        return;
    }


    try {
        await apiRequest('/api/admin/telegram-settings', {
            method: 'POST',
            body: JSON.stringify({
                bot_token: botToken,
                chat_id: chatId,
                enable_notifications: enableNotifications // Use the potentially modified value
            })
        });

        showToast('success', 'Telegram设置已成功保存');

    } catch (error) {
            showToast('danger', '保存Telegram设置失败: ' + error.message);
    }
}

// --- ntfy Settings Functions ---

// 加载ntfy通知设置
async function loadNtfySettings() {
    try {
        const settings = await apiRequest('/api/admin/ntfy-settings');
        if (settings) {
            document.getElementById('ntfyTopic').value = settings.topic || '';
            document.getElementById('ntfyServerUrl').value = settings.server_url || 'https://ntfy.sh';
            document.getElementById('enableNtfyNotifications').checked = !!settings.enable_notifications;
        }
    } catch (error) {
                showToast('danger', '加载ntfy设置失败: ' + error.message);
    }
}

// 保存ntfy通知设置
async function saveNtfySettings() {
    const topic = document.getElementById('ntfyTopic').value.trim();
    const serverUrl = document.getElementById('ntfyServerUrl').value.trim();
    let enableNotifications = document.getElementById('enableNtfyNotifications').checked;

    if (!topic) {
        enableNotifications = false;
        document.getElementById('enableNtfyNotifications').checked = false;
        if (document.getElementById('enableNtfyNotifications').checked) {
            showToast('warning', 'Topic 不能为空才能启用通知。通知已自动禁用');
        }
    } else if (enableNotifications && !topic) {
        showToast('warning', '启用通知时，Topic 不能为空');
        return;
    }

    try {
        await apiRequest('/api/admin/ntfy-settings', {
            method: 'POST',
            body: JSON.stringify({
                topic: topic,
                server_url: serverUrl || 'https://ntfy.sh',
                enable_notifications: enableNotifications
            })
        });

        showToast('success', 'ntfy设置已成功保存');

    } catch (error) {
            showToast('danger', '保存ntfy设置失败: ' + error.message);
    }
}

// --- Background Settings Functions ---

// 加载背景设置
async function loadBackgroundSettings() {
    try {
        const settings = await apiRequest('/api/background-settings');
        if (settings) {
            document.getElementById('enableCustomBackground').checked = !!settings.enabled;
            document.getElementById('backgroundImageUrl').value = settings.url || '';
            document.getElementById('pageOpacity').value = settings.opacity || 80;
            document.getElementById('opacityValue').textContent = settings.opacity || 80;

            // 应用当前设置（不保存到数据库）
            applyBackgroundSettings(settings.enabled, settings.url, settings.opacity, false);
        }
    } catch (error) {
                showToast('danger', '加载背景设置失败: ' + error.message);
    }
}

// 保存背景设置
async function saveBackgroundSettings() {
    const enabled = document.getElementById('enableCustomBackground').checked;
    const url = document.getElementById('backgroundImageUrl').value.trim();
    const opacity = parseInt(document.getElementById('pageOpacity').value, 10);

    // 验证输入
    if (enabled && url) {
        if (!url.startsWith('https://')) {
            showToast('warning', '背景图片URL必须以https://开头');
            return;
        }
    }

    if (isNaN(opacity) || opacity < 0 || opacity > 100) {
        showToast('warning', '透明度必须是0-100之间的数字');
        return;
    }

    try {
        await apiRequest('/api/admin/background-settings', {
            method: 'POST',
            body: JSON.stringify({
                enabled: enabled,
                url: url,
                opacity: opacity
            })
        });

        // 应用设置并保存到localStorage
        applyBackgroundSettings(enabled, url, opacity, true);

        showToast('success', '背景设置已成功保存');

    } catch (error) {
                showToast('danger', '保存背景设置失败: ' + error.message);
    }
}

// 应用背景设置
function applyBackgroundSettings(enabled, url, opacity, saveToCache = false) {
    const body = document.body;

    if (enabled && url) {
        // 设置背景图片
        body.style.setProperty('--custom-background-url', \`url(\${url})\`);
        body.style.setProperty('--page-opacity', opacity / 100);
        body.classList.add('custom-background-enabled');


    } else {
        // 移除背景图片
        body.style.removeProperty('--custom-background-url');
        body.style.removeProperty('--page-opacity');
        body.classList.remove('custom-background-enabled');


    }

    // 缓存设置到localStorage（可选）
    if (saveToCache) {
        const settings = { enabled, url, opacity, timestamp: Date.now() };
        localStorage.setItem('background-settings-cache', JSON.stringify(settings));
    }
}

// 实时预览透明度变化
function updateOpacityPreview() {
    const opacity = parseInt(document.getElementById('pageOpacity').value, 10);
    const enabled = document.getElementById('enableCustomBackground').checked;
    const url = document.getElementById('backgroundImageUrl').value.trim();

    // 更新显示的数值
    document.getElementById('opacityValue').textContent = opacity;

    // 实时预览（不保存）
    if (enabled && url) {
        document.body.style.setProperty('--page-opacity', opacity / 100);

    }
}



// --- Global Settings Functions (VPS Report Interval) ---
async function loadGlobalSettings() {
    try {
        const settings = await apiRequest('/api/admin/settings/vps-report-interval');
        if (settings && typeof settings.interval === 'number') {
            document.getElementById('vpsReportInterval').value = settings.interval;
        } else {
            document.getElementById('vpsReportInterval').value = 60; // Default if not set
        }
    } catch (error) {
                showToast('danger', '加载VPS报告间隔失败: ' + error.message);
        document.getElementById('vpsReportInterval').value = 60; // Default on error
    }
}

async function saveVpsReportInterval() {
    const intervalInput = document.getElementById('vpsReportInterval');
    const interval = parseInt(intervalInput.value, 10);

    if (isNaN(interval) || interval < 1) { // Changed to interval < 1
        showToast('warning', 'VPS报告间隔必须是一个大于或等于1的数字');
        return;
    }
    // Removed warning for interval < 10

    try {
        await apiRequest('/api/admin/settings/vps-report-interval', {
            method: 'POST',
            body: JSON.stringify({ interval: interval })
        });

        showToast('success', 'VPS数据更新频率已成功保存。前端刷新间隔已立即更新');

        // Immediately update the frontend refresh interval
        // Check if we're on a page that has VPS data updates running
        if (typeof initializeVpsDataUpdates === 'function') {
            try {
                await initializeVpsDataUpdates();
                            } catch (error) {
                            }
        }
    } catch (error) {
                showToast('danger', '保存VPS报告间隔失败: ' + error.message);
    }
}

// --- 自动排序功能 ---

// 服务器自动排序
async function autoSortServers(sortBy) {
    try {
        await apiRequest('/api/admin/servers/auto-sort', {
            method: 'POST',
            body: JSON.stringify({ sortBy: sortBy, order: 'asc' })
        });

        // 更新下拉菜单选中状态
        updateServerSortDropdownSelection(sortBy);

        // 重新加载服务器列表
        await loadServerList();
        showToast('success', '服务器已按' + getSortDisplayName(sortBy) + '排序');

    } catch (error) {
                showToast('danger', '服务器自动排序失败: ' + error.message);
    }
}

// 网站自动排序
async function autoSortSites(sortBy) {
    try {
        await apiRequest('/api/admin/sites/auto-sort', {
            method: 'POST',
            body: JSON.stringify({ sortBy: sortBy, order: 'asc' })
        });

        // 更新下拉菜单选中状态
        updateSiteSortDropdownSelection(sortBy);

        // 重新加载网站列表
        await loadSiteList();
        showToast('success', '网站已按' + getSortDisplayName(sortBy) + '排序');

    } catch (error) {
                showToast('danger', '网站自动排序失败: ' + error.message);
    }
}

// 获取排序字段的显示名称
function getSortDisplayName(sortBy) {
    const displayNames = {
        'custom': '自定义',
        'name': '名称',
        'status': '状态',
        'created_at': '创建时间',
        'added_at': '添加时间',
        'url': 'URL'
    };
    return displayNames[sortBy] || sortBy;
}

// 更新服务器排序下拉菜单选中状态
function updateServerSortDropdownSelection(selectedSortBy) {
    const dropdown = document.querySelector('#serverAutoSortDropdown + .dropdown-menu');
    if (!dropdown) return;

    // 移除所有active类
    dropdown.querySelectorAll('.dropdown-item').forEach(item => {
        item.classList.remove('active');
    });

    // 为选中的项添加active类
    const selectedItem = dropdown.querySelector(\`[onclick="autoSortServers('\${selectedSortBy}')"]\`);
    if (selectedItem) {
        selectedItem.classList.add('active');
    }
}

// 更新网站排序下拉菜单选中状态
function updateSiteSortDropdownSelection(selectedSortBy) {
    const dropdown = document.querySelector('#siteAutoSortDropdown + .dropdown-menu');
    if (!dropdown) return;

    // 移除所有active类
    dropdown.querySelectorAll('.dropdown-item').forEach(item => {
        item.classList.remove('active');
    });

    // 为选中的项添加active类
    const selectedItem = dropdown.querySelector(\`[onclick="autoSortSites('\${selectedSortBy}')"]\`);
    if (selectedItem) {
        selectedItem.classList.add('active');
    }
}

// 管理页面移动端服务器卡片渲染函数
function renderMobileAdminServerCards(servers) {
    const mobileContainer = document.getElementById('mobileAdminServerContainer');
    if (!mobileContainer) return;

    mobileContainer.innerHTML = '';

    if (!servers || servers.length === 0) {
        mobileContainer.innerHTML = '<div class="text-center p-3 text-muted">暂无服务器数据</div>';
        return;
    }

    servers.forEach(server => {
        const card = document.createElement('div');
        card.className = 'mobile-server-card';
        card.setAttribute('data-server-id', server.id);

        // 状态显示逻辑（与PC端一致）
        let statusBadge = '<span class="badge bg-secondary">未知</span>';
        let lastUpdateText = '从未';

        if (server.last_report) {
            const lastUpdate = new Date(server.last_report * 1000);
            lastUpdateText = lastUpdate.toLocaleString();

            // 检查是否在线（最后报告时间在5分钟内）
            const now = new Date();
            const diffMinutes = (now - lastUpdate) / (1000 * 60);

            if (diffMinutes <= 5) {
                statusBadge = '<span class="badge bg-success">在线</span>';
            } else {
                statusBadge = '<span class="badge bg-danger">离线</span>';
            }
        }

        // 卡片头部
        const cardHeader = document.createElement('div');
        cardHeader.className = 'mobile-card-header';
        cardHeader.innerHTML = \`
            <div class="mobile-card-header-left">
                \${statusBadge}
            </div>
            <h6 class="mobile-card-title text-center">\${server.name || '未命名服务器'}</h6>
            <div class="mobile-card-header-right">
                <span class="me-2">显示</span>
                <div class="form-check form-switch d-inline-block">
                    <input class="form-check-input server-visibility-toggle" type="checkbox"
                           data-server-id="\${server.id}" \${server.is_public ? 'checked' : ''}>
                </div>
            </div>
        \`;

        // 卡片主体
        const cardBody = document.createElement('div');
        cardBody.className = 'mobile-card-body';

        // 描述 - 单行
        if (server.description) {
            const descRow = document.createElement('div');
            descRow.className = 'mobile-card-row';
            descRow.innerHTML = \`
                <span class="mobile-card-label">描述</span>
                <span class="mobile-card-value">\${server.description}</span>
            \`;
            cardBody.appendChild(descRow);
        }



        // 四个按钮 - 两行两列布局
        const buttonsContainer = document.createElement('div');
        buttonsContainer.className = 'mobile-card-buttons-grid';
        buttonsContainer.innerHTML = \`
            <div class="d-flex gap-2 mb-2">
                <button class="btn btn-outline-secondary btn-sm flex-fill" onclick="showServerApiKey('\${server.id}')">
                    <i class="bi bi-key"></i> 查看密钥
                </button>
                <button class="btn btn-outline-info btn-sm flex-fill" onclick="copyVpsInstallScript('\${server.id}', '\${server.name}', this)">
                    <i class="bi bi-clipboard"></i> 复制脚本
                </button>
            </div>
            <div class="d-flex gap-2">
                <button class="btn btn-outline-primary btn-sm flex-fill" onclick="editServer('\${server.id}')">
                    <i class="bi bi-pencil"></i> 编辑
                </button>
                <button class="btn btn-outline-danger btn-sm flex-fill" onclick="deleteServer('\${server.id}')">
                    <i class="bi bi-trash"></i> 删除
                </button>
            </div>
        \`;
        cardBody.appendChild(buttonsContainer);

        // 最后更新时间 - 底部单行（与PC端功能一致）
        const lastUpdateRow = document.createElement('div');
        lastUpdateRow.className = 'mobile-card-row mobile-card-footer';
        lastUpdateRow.innerHTML = \`
            <span class="mobile-card-label">最后更新: \${lastUpdateText}</span>
        \`;
        cardBody.appendChild(lastUpdateRow);

        // 组装卡片
        card.appendChild(cardHeader);
        card.appendChild(cardBody);

        mobileContainer.appendChild(card);
    });

    // 为移动端显示开关添加事件监听器
    document.querySelectorAll('.server-visibility-toggle').forEach(toggle => {
        toggle.addEventListener('change', function() {
            const serverId = this.dataset.serverId;
            const isPublic = this.checked;
            toggleServerVisibility(serverId, isPublic);
        });
    });
}

// 切换服务器显示状态
async function toggleServerVisibility(serverId, isPublic) {
    try {
        const toggle = document.querySelector(\`.server-visibility-toggle[data-server-id="\${serverId}"]\`);
        if (toggle) {
            toggle.disabled = true;
            toggle.style.opacity = '0.6';
        }

        await apiRequest(\`/api/admin/servers/\${serverId}/visibility\`, {
            method: 'POST',
            body: JSON.stringify({ is_public: isPublic })
        });

        // 更新本地数据
        const serverIndex = serverList.findIndex(s => s.id === serverId);
        if (serverIndex !== -1) {
            serverList[serverIndex].is_public = isPublic;
        }

        if (toggle) {
            toggle.disabled = false;
            toggle.style.opacity = '1';
        }

        showToast('success', '服务器显示状态已' + (isPublic ? '开启' : '关闭'));

    } catch (error) {
                // 恢复开关状态
        const toggle = document.querySelector(\`.server-visibility-toggle[data-server-id="\${serverId}"]\`);
        if (toggle) {
            toggle.checked = !isPublic;
            toggle.disabled = false;
            toggle.style.opacity = '1';
        }

        showToast('danger', '切换显示状态失败: ' + error.message);
    }
}

// 管理页面移动端网站卡片渲染函数
function renderMobileAdminSiteCards(sites) {
    const mobileContainer = document.getElementById('mobileAdminSiteContainer');
    if (!mobileContainer) return;

    mobileContainer.innerHTML = '';

    // 添加居中的排序和添加网站按钮
    const mobileActionsContainer = document.createElement('div');
    mobileActionsContainer.className = 'text-center mb-3';
    mobileActionsContainer.innerHTML = \`
        <div class="d-flex gap-2 justify-content-center">
            <div class="dropdown">
                <button class="btn btn-outline-secondary dropdown-toggle" type="button" data-bs-toggle="dropdown" aria-expanded="false">
                    <i class="bi bi-sort-alpha-down"></i> 自动排序
                </button>
                <ul class="dropdown-menu">
                    <li><a class="dropdown-item active" href="#" onclick="autoSortSites('custom')">自定义排序</a></li>
                    <li><a class="dropdown-item" href="#" onclick="autoSortSites('name')">按名称排序</a></li>
                    <li><a class="dropdown-item" href="#" onclick="autoSortSites('url')">按URL排序</a></li>
                    <li><a class="dropdown-item" href="#" onclick="autoSortSites('status')">按状态排序</a></li>
                </ul>
            </div>
            <button id="addSiteBtnMobile" class="btn btn-success" onclick="showSiteModal()">
                <i class="bi bi-plus-circle"></i> 添加监控网站
            </button>
        </div>
    \`;
    mobileContainer.appendChild(mobileActionsContainer);

    if (!sites || sites.length === 0) {
        const noDataDiv = document.createElement('div');
        noDataDiv.className = 'text-center p-3 text-muted';
        noDataDiv.textContent = '暂无监控网站数据';
        mobileContainer.appendChild(noDataDiv);
        return;
    }

    sites.forEach(site => {
        const card = document.createElement('div');
        card.className = 'mobile-site-card';

        const statusInfo = getSiteStatusBadge(site.last_status);
        const lastCheckTime = site.last_checked ? new Date(site.last_checked * 1000).toLocaleString() : '从未';
        const responseTime = site.last_response_time_ms !== null ? \`\${site.last_response_time_ms} ms\` : '-';

        // 卡片头部 - 完全参考服务器卡片布局：状态在左上角，网站名在中间，显示开关在右上角
        const cardHeader = document.createElement('div');
        cardHeader.className = 'mobile-card-header';
        cardHeader.innerHTML = \`
            <div class="mobile-card-header-left">
                <span class="badge \${statusInfo.class}">\${statusInfo.text}</span>
            </div>
            <h6 class="mobile-card-title text-center">\${site.name || '未命名网站'}</h6>
            <div class="mobile-card-header-right">
                <span class="me-2">显示</span>
                <div class="form-check form-switch d-inline-block">
                    <input class="form-check-input site-visibility-toggle" type="checkbox"
                           data-site-id="\${site.id}" \${site.is_public ? 'checked' : ''}>
                </div>
            </div>
        \`;

        // 卡片主体
        const cardBody = document.createElement('div');
        cardBody.className = 'mobile-card-body';

        // URL 和网站链接 - 单行
        const urlRow = document.createElement('div');
        urlRow.className = 'mobile-card-row';
        urlRow.innerHTML = \`
            <span class="mobile-card-label" style="word-break: break-all;">
                URL: \${site.url}<a href="\${site.url}" target="_blank" rel="noopener noreferrer" class="text-decoration-none" style="margin-left: 4px;"><i class="bi bi-box-arrow-up-right"></i></a>
            </span>
        \`;
        cardBody.appendChild(urlRow);



        // 最后检查 - 单行
        const lastCheckRow = document.createElement('div');
        lastCheckRow.className = 'mobile-card-row';
        lastCheckRow.innerHTML = \`
            <span class="mobile-card-label">最后检查: \${lastCheckTime}</span>
        \`;
        cardBody.appendChild(lastCheckRow);

        // 操作按钮 - 编辑和删除
        const actionsRow = document.createElement('div');
        actionsRow.className = 'mobile-card-row';
        actionsRow.innerHTML = \`
            <div class="d-flex gap-2 w-100">
                <button class="btn btn-outline-primary btn-sm flex-fill" onclick="editSite('\${site.id}')">
                    <i class="bi bi-pencil"></i> 编辑
                </button>
                <button class="btn btn-outline-danger btn-sm flex-fill" onclick="deleteSite('\${site.id}')">
                    <i class="bi bi-trash"></i> 删除
                </button>
            </div>
        \`;
        cardBody.appendChild(actionsRow);

        // 组装卡片
        card.appendChild(cardHeader);
        card.appendChild(cardBody);

        mobileContainer.appendChild(card);
    });

    // 为移动端网站显示开关添加事件监听器
    document.querySelectorAll('.site-visibility-toggle').forEach(toggle => {
        toggle.addEventListener('change', function() {
            const siteId = this.dataset.siteId;
            const isPublic = this.checked;
            toggleSiteVisibility(siteId, isPublic);
        });
    });
}

// 切换网站显示状态
async function toggleSiteVisibility(siteId, isPublic) {
    try {
        const toggle = document.querySelector(\`.site-visibility-toggle[data-site-id="\${siteId}"]\`);
        if (toggle) {
            toggle.disabled = true;
            toggle.style.opacity = '0.6';
        }

        await apiRequest(\`/api/admin/sites/\${siteId}/visibility\`, {
            method: 'POST',
            body: JSON.stringify({ is_public: isPublic })
        });

        // 更新本地数据
        const siteIndex = siteList.findIndex(s => s.id === siteId);
        if (siteIndex !== -1) {
            siteList[siteIndex].is_public = isPublic;
        }

        if (toggle) {
            toggle.disabled = false;
            toggle.style.opacity = '1';
        }

        showToast('success', '网站显示状态已' + (isPublic ? '开启' : '关闭'));

    } catch (error) {
                // 恢复开关状态
        const toggle = document.querySelector(\`.site-visibility-toggle[data-site-id="\${siteId}"]\`);
        if (toggle) {
            toggle.checked = !isPublic;
            toggle.disabled = false;
            toggle.style.opacity = '1';
        }

        showToast('danger', '切换显示状态失败: ' + error.message);
    }
}

// 移动端查看服务器API密钥
function showServerApiKey(serverId) {
    viewApiKey(serverId);
}

// 渲染移动端LLM管理卡片
function renderMobileAdminLlmCards(endpoints) {
    const mobileContainer = document.getElementById('mobileAdminLlmContainer');
    if (!mobileContainer) return;

    mobileContainer.innerHTML = '';

    if (!endpoints || endpoints.length === 0) {
        mobileContainer.innerHTML = '<div class="text-center p-3 text-muted">暂无LLM端点数据</div>';
        return;
    }

    endpoints.forEach(ep => {
        const card = document.createElement('div');
        card.className = 'card mb-2';

        const statusInfo = getSiteStatusBadge(ep.last_status);
        const lastCheckTime = ep.last_checked ? new Date(ep.last_checked * 1000).toLocaleString() : '从未';
        const responseTime = ep.last_response_time_ms !== null ? \`\${ep.last_response_time_ms} ms\` : '-';

        card.innerHTML = \`
            <div class="card-body p-2">
                <div class="d-flex justify-content-between align-items-center">
                    <strong>\${ep.model}</strong>
                    <span class="badge \${statusInfo.class}">\${statusInfo.text}</span>
                </div>
                <div class="mt-1"><small>响应: \${responseTime} | \${lastCheckTime}</small></div>
                <div class="mt-2 d-flex justify-content-between align-items-center">
                    <div class="form-check form-switch">
                        <input class="form-check-input llm-visibility-toggle" type="checkbox" data-llm-id="\${ep.id}" \${ep.is_public ? 'checked' : ''}>
                        <label class="form-check-label ms-1">显示</label>
                    </div>
                    <div class="form-check form-switch">
                        <input class="form-check-input llm-notification-toggle" type="checkbox" data-llm-id="\${ep.id}" \${ep.enable_notifications !== 0 ? 'checked' : ''}>
                        <label class="form-check-label ms-1">通知</label>
                    </div>
                </div>
                <div class="mt-2 d-flex gap-2">
                    <button class="btn btn-outline-primary btn-sm flex-fill edit-llm-btn" data-id="\${ep.id}">
                        <i class="bi bi-pencil"></i> 编辑
                    </button>
                    <button class="btn btn-outline-danger btn-sm flex-fill delete-llm-btn" data-id="\${ep.id}" data-name="\${ep.model}">
                        <i class="bi bi-trash"></i> 删除
                    </button>
                </div>
            </div>
        \`;

        mobileContainer.appendChild(card);
    });

    // 绑定移动端事件 - 限域到 mobileContainer
    mobileContainer.querySelectorAll('.edit-llm-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            showLlmEndpointModal(this.getAttribute('data-id'));
        });
    });

    mobileContainer.querySelectorAll('.delete-llm-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            showDeleteLlmEndpointConfirmation(this.getAttribute('data-id'), this.getAttribute('data-name'));
        });
    });

    mobileContainer.querySelectorAll('.llm-visibility-toggle').forEach(toggle => {
        toggle.addEventListener('change', async function() {
            const endpointId = this.getAttribute('data-llm-id');
            const isPublic = this.checked;
            try {
                await apiRequest(\`/api/admin/llm-endpoints/\${endpointId}/visibility\`, {
                    method: 'PUT',
                    body: JSON.stringify({ is_public: isPublic })
                });
                showToast('success', '可见性更新成功');
            } catch (error) {
                this.checked = !this.checked;
                showToast('danger', '更新可见性失败: ' + error.message);
            }
        });
    });

    mobileContainer.querySelectorAll('.llm-notification-toggle').forEach(toggle => {
        toggle.addEventListener('change', async function() {
            const endpointId = this.getAttribute('data-llm-id');
            const enableNotifications = this.checked;
            try {
                await apiRequest(\`/api/admin/llm-endpoints/\${endpointId}/notifications\`, {
                    method: 'PUT',
                    body: JSON.stringify({ enable_notifications: enableNotifications })
                });
                showToast('success', '通知设置更新成功');
            } catch (error) {
                this.checked = !this.checked;
                showToast('danger', '更新通知设置失败: ' + error.message);
            }
        });
    });
}

function renderGroupedAdminLlmTable(endpoints) {
    const tableBody = document.getElementById('llmEndpointTableBody');
    if (!tableBody) return;

    tableBody.innerHTML = '';

    if (endpoints.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="10" class="text-center">暂无LLM端点</td></tr>';
        return;
    }

    const providerGroups = groupLlmEndpointsByProvider(endpoints);
    providerGroups.forEach((group) => {
        const expanded = llmProviderExpansionState[group.key] !== false;
        const providerStatus = getProviderStatusInfo(group);
        const providerHeader = getProviderHeaderMeta(group);
        const groupRow = document.createElement('tr');
        groupRow.className = 'provider-group-row';
        groupRow.innerHTML = \`
            <td colspan="10">
                <div class="d-flex justify-content-between align-items-start gap-3">
                    <div>
                        <button type="button" class="btn btn-sm btn-link text-decoration-none p-0 admin-provider-toggle" data-provider-key="\${escapeHtml(group.key)}">
                            <i class="bi \${expanded ? 'bi-caret-down-fill' : 'bi-caret-right-fill'} me-1"></i>
                            <strong>\${escapeHtml(providerHeader.title)}</strong>
                        </button>
                        <div><small class="text-muted">\${escapeHtml(providerHeader.subtitle)}</small></div>
                    </div>
                    <div class="d-flex align-items-center gap-2 flex-shrink-0">
                        <small class="text-muted">\${group.endpoints.length} 个模型</small>
                        <span class="badge \${providerStatus.class}">\${providerStatus.text}</span>
                    </div>
                </div>
            </td>
        \`;
        tableBody.appendChild(groupRow);

        group.endpoints.forEach((ep) => {
            const row = document.createElement('tr');
            row.setAttribute('data-llm-id', ep.id);
            row.dataset.providerKey = group.key;
            row.classList.add('provider-child-row');
            row.classList.add('llm-row-draggable');
            row.draggable = true;
            row.style.display = expanded ? '' : 'none';

            const statusInfo = getSiteStatusBadge(ep.last_status);
            const lastCheckTime = ep.last_checked ? new Date(ep.last_checked * 1000).toLocaleString() : '从未';
            const responseTime = ep.last_response_time_ms !== null ? \`\${ep.last_response_time_ms} ms\` : '-';

            row.innerHTML = \`
                <td><i class="bi bi-grip-vertical text-muted me-2" style="cursor: grab;" title="拖拽排序"></i></td>
                <td><span class="ms-3 badge bg-info-subtle text-dark border">\${escapeHtml(ep.model)}</span></td>
                <td><a href="\${escapeHtml(ep.api_url)}" target="_blank" rel="noopener noreferrer" class="text-truncate d-inline-block" style="max-width:200px;">\${escapeHtml(ep.api_url)}</a></td>
                <td><span class="badge \${statusInfo.class}">\${statusInfo.text}</span></td>
                <td>\${ep.last_status_code || '-'}</td>
                <td>\${responseTime}</td>
                <td>\${lastCheckTime}</td>
                <td>
                    <div class="form-check form-switch">
                        <input class="form-check-input llm-visibility-toggle" type="checkbox" data-llm-id="\${ep.id}" \${ep.is_public ? 'checked' : ''}>
                    </div>
                </td>
                <td>
                    <div class="form-check form-switch">
                        <input class="form-check-input llm-notification-toggle" type="checkbox" data-llm-id="\${ep.id}" \${ep.enable_notifications !== 0 ? 'checked' : ''}>
                    </div>
                </td>
                <td>
                    <div class="btn-group">
                        <button class="btn btn-sm btn-outline-primary edit-llm-btn" data-id="\${ep.id}" title="编辑">
                            <i class="bi bi-pencil"></i>
                        </button>
                        <button class="btn btn-sm btn-outline-danger delete-llm-btn" data-id="\${ep.id}" data-name="\${escapeHtml(ep.model)}" title="删除">
                            <i class="bi bi-trash"></i>
                        </button>
                    </div>
                </td>
            \`;
            tableBody.appendChild(row);
        });
    });

    bindAdminLlmEvents(tableBody);
    initializeLlmDragSort();
}

function renderGroupedMobileAdminLlmCards(endpoints) {
    const mobileContainer = document.getElementById('mobileAdminLlmContainer');
    if (!mobileContainer) return;

    mobileContainer.innerHTML = '';

    if (!endpoints || endpoints.length === 0) {
        mobileContainer.innerHTML = '<div class="text-center p-3 text-muted">暂无LLM端点数据</div>';
        return;
    }

    const providerGroups = groupLlmEndpointsByProvider(endpoints);
    providerGroups.forEach(group => {
        const expanded = llmProviderExpansionState[group.key] !== false;
        const providerStatus = getProviderStatusInfo(group);
        const providerHeader = getProviderHeaderMeta(group);
        const card = document.createElement('div');
        card.className = 'card mb-2';

        const cardBody = document.createElement('div');
        cardBody.className = 'card-body p-2';
        cardBody.innerHTML = \`
            <div class="d-flex justify-content-between align-items-center">
                <button type="button" class="btn btn-sm btn-link text-decoration-none p-0 admin-provider-toggle" data-provider-key="\${escapeHtml(group.key)}">
                    <i class="bi \${expanded ? 'bi-caret-down-fill' : 'bi-caret-right-fill'} me-1"></i><strong>\${escapeHtml(providerHeader.title)}</strong>
                </button>
                <span class="badge \${providerStatus.class}">\${providerStatus.text}</span>
            </div>
            <div class="mt-1 mb-2"><small class="text-muted">\${escapeHtml(providerHeader.subtitle)}</small></div>
        \`;

        group.endpoints.forEach(ep => {
            const statusInfo = getSiteStatusBadge(ep.last_status);
            const lastCheckTime = ep.last_checked ? new Date(ep.last_checked * 1000).toLocaleString() : '从未';
            const responseTime = ep.last_response_time_ms !== null ? \`\${ep.last_response_time_ms} ms\` : '-';
            const item = document.createElement('div');
            item.dataset.providerKey = group.key;
            item.style.display = expanded ? '' : 'none';
            item.className = 'border-top pt-2 mt-2';
            item.innerHTML = \`
                <div class="d-flex justify-content-between align-items-center">
                    <strong>\${escapeHtml(ep.model)}</strong>
                    <span class="badge \${statusInfo.class}">\${statusInfo.text}</span>
                </div>
                <div class="mt-1"><small>响应: \${responseTime} | \${lastCheckTime}</small></div>
                <div class="mt-2 d-flex justify-content-between align-items-center">
                    <div class="form-check form-switch">
                        <input class="form-check-input llm-visibility-toggle" type="checkbox" data-llm-id="\${ep.id}" \${ep.is_public ? 'checked' : ''}>
                        <label class="form-check-label ms-1">显示</label>
                    </div>
                    <div class="form-check form-switch">
                        <input class="form-check-input llm-notification-toggle" type="checkbox" data-llm-id="\${ep.id}" \${ep.enable_notifications !== 0 ? 'checked' : ''}>
                        <label class="form-check-label ms-1">通知</label>
                    </div>
                </div>
                <div class="mt-2 d-flex gap-2">
                    <button class="btn btn-outline-primary btn-sm flex-fill edit-llm-btn" data-id="\${ep.id}">
                        <i class="bi bi-pencil"></i> 编辑
                    </button>
                    <button class="btn btn-outline-danger btn-sm flex-fill delete-llm-btn" data-id="\${ep.id}" data-name="\${escapeHtml(ep.model)}">
                        <i class="bi bi-trash"></i> 删除
                    </button>
                </div>
            \`;
            cardBody.appendChild(item);
        });

        card.appendChild(cardBody);
        mobileContainer.appendChild(card);
    });

    bindAdminLlmEvents(mobileContainer);
}

function bindAdminLlmEvents(container) {
    container.querySelectorAll('.admin-provider-toggle').forEach(btn => {
        btn.addEventListener('click', function() {
            const providerKey = this.getAttribute('data-provider-key');
            llmProviderExpansionState[providerKey] = !(llmProviderExpansionState[providerKey] !== false);
            renderGroupedAdminLlmTable(llmEndpointList);
            renderGroupedMobileAdminLlmCards(llmEndpointList);
        });
    });

    container.querySelectorAll('.edit-llm-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            showLlmEndpointModal(this.getAttribute('data-id'));
        });
    });

    container.querySelectorAll('.delete-llm-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            showDeleteLlmEndpointConfirmation(this.getAttribute('data-id'), this.getAttribute('data-name'));
        });
    });

    container.querySelectorAll('.llm-visibility-toggle').forEach(toggle => {
        toggle.addEventListener('change', async function() {
            const endpointId = this.getAttribute('data-llm-id');
            const isPublic = this.checked;
            try {
                await apiRequest(\`/api/admin/llm-endpoints/\${endpointId}/visibility\`, {
                    method: 'PUT',
                    body: JSON.stringify({ is_public: isPublic })
                });
                showToast('success', '可见性更新成功');
            } catch (error) {
                this.checked = !this.checked;
                showToast('danger', '更新可见性失败: ' + error.message);
            }
        });
    });

    container.querySelectorAll('.llm-notification-toggle').forEach(toggle => {
        toggle.addEventListener('change', async function() {
            const endpointId = this.getAttribute('data-llm-id');
            const enableNotifications = this.checked;
            try {
                await apiRequest(\`/api/admin/llm-endpoints/\${endpointId}/notifications\`, {
                    method: 'PUT',
                    body: JSON.stringify({ enable_notifications: enableNotifications })
                });
                showToast('success', '通知设置更新成功');
            } catch (error) {
                this.checked = !this.checked;
                showToast('danger', '更新通知设置失败: ' + error.message);
            }
        });
    });
}

// ==================== 全局背景设置同步功能 ====================

// 监听storage事件，实现跨页面设置同步
window.addEventListener('storage', function(e) {
    if (e.key === 'background-settings-cache' && e.newValue) {
        try {
            const newSettings = JSON.parse(e.newValue);
            // 使用管理页面的背景设置应用函数
            applyBackgroundSettings(newSettings.enabled, newSettings.url, newSettings.opacity, false);
                    } catch (error) {
                    }
    }
});

// 页面加载时也检查并应用缓存的背景设置
document.addEventListener('DOMContentLoaded', function() {
    // 延迟执行，确保loadBackgroundSettings()先执行
    setTimeout(function() {
        const cached = localStorage.getItem('background-settings-cache');
        if (cached) {
            try {
                const cachedData = JSON.parse(cached);
                const now = Date.now();
                const cacheAge = now - cachedData.timestamp;
                const CACHE_DURATION = 5 * 60 * 1000; // 5分钟缓存

                if (cacheAge < CACHE_DURATION) {
                    // 缓存有效，确保设置已应用
                    applyBackgroundSettings(cachedData.enabled, cachedData.url, cachedData.opacity, false);
                                    }
            } catch (error) {
                            }
        }
    }, 100);
});
`;
}
