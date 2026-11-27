/**
 * @param  {String}  url
 * @param  {Boolean} isNoCaseSensitive 是否区分大小写
 * @return {Object}
 */
// import numeral from 'numeral';

import forge from 'node-forge';

export const getWidth = () => {
  return { width: window.innerWidth };
};

export const rsaPsw = async (password: string): Promise<string> => {
  // Fetch for each credential operation so a new site or a rotated key never
  // needs a frontend rebuild. Failure must not fall back to plaintext or a key
  // embedded in an old release.
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10000);
  let result;
  try {
    const response = await fetch('/v1/user/public-key', {
      credentials: 'same-origin',
      cache: 'no-store',
      signal: controller.signal,
    });
    if (!response.ok) throw new Error('登录加密配置不可用，请稍后重试');
    result = await response.json();
  } finally {
    clearTimeout(timeout);
  }
  const data = result?.data;
  if (
    result?.code !== 0 ||
    data?.algorithm !== 'RSA-OAEP-256' ||
    typeof data?.public_key !== 'string' ||
    !/^[a-f0-9]{64}$/.test(data?.key_id || '')
  ) {
    throw new Error('登录加密配置无效，请联系管理员');
  }
  const publicKey = forge.pki.publicKeyFromPem(data.public_key);
  if (publicKey.n.bitLength() < 2048) {
    throw new Error('登录加密配置无效，请联系管理员');
  }

  // 使用RSA-OAEP（SHA-256）加密，并返回Base64
  const encryptedBinary = publicKey.encrypt(
    forge.util.encodeUtf8(password),
    'RSA-OAEP',
    {
      md: forge.md.sha256.create(),
      mgf1: forge.mgf1.create(forge.md.sha256.create()),
    },
  );
  return forge.util.encode64(encryptedBinary);
};

export default {
  getWidth,
  rsaPsw,
};

export const getFileExtension = (filename: string) =>
  filename.slice(filename.lastIndexOf('.') + 1).toLowerCase();
