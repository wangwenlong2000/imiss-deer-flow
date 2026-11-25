import { Authorization, Token, UserInfo } from '@/constants/authorization';
import { getSearchValue } from './common-util';
import { notifyAuthorizationChanged } from './session';
const KeySet = [Authorization, Token, UserInfo];

const storage = {
  getAuthorization: () => {
    return localStorage.getItem(Authorization);
  },
  getToken: () => {
    return localStorage.getItem(Token);
  },
  getUserInfo: () => {
    return localStorage.getItem(UserInfo);
  },
  getUserInfoObject: () => {
    return JSON.parse(localStorage.getItem('userInfo') || '');
  },
  setAuthorization: (value: string) => {
    const previous = localStorage.getItem(Authorization);
    localStorage.setItem(Authorization, value);
    if (previous !== value) notifyAuthorizationChanged();
  },
  setToken: (value: string) => {
    localStorage.setItem(Token, value);
  },
  setUserInfo: (value: string | Record<string, unknown>) => {
    let valueStr = typeof value !== 'string' ? JSON.stringify(value) : value;
    localStorage.setItem(UserInfo, valueStr);
  },
  setItems: (pairs: Record<string, string>) => {
    const previous = localStorage.getItem(Authorization);
    Object.entries(pairs).forEach(([key, value]) => {
      localStorage.setItem(key, value);
    });
    if (previous !== localStorage.getItem(Authorization)) notifyAuthorizationChanged();
  },
  removeAuthorization: () => {
    const previous = localStorage.getItem(Authorization);
    localStorage.removeItem(Authorization);
    if (previous !== null) notifyAuthorizationChanged();
  },
  removeAll: () => {
    const previous = localStorage.getItem(Authorization);
    KeySet.forEach((x) => {
      localStorage.removeItem(x);
    });
    localStorage.removeItem('Token'); // Remove the old login key as well.
    if (previous !== null) notifyAuthorizationChanged();
  },
  setLanguage: (lng: string) => {
    localStorage.setItem('lng', lng);
  },
  getLanguage: (): string => {
    return localStorage.getItem('lng') as string;
  },
};

export const getAuthorization = () => {
  const sharedId = getSearchValue('shared_id');
  const authorization = sharedId
    ? 'Bearer ' + sharedId
    : storage.getAuthorization() || '';

  return authorization;
};

// A public shared-dialog token must never replace the platform login identity.
export const getPlatformAuthorization = () => storage.getAuthorization() || '';

export const getUserInfoObject = () => {
  if (storage.getUserInfo()) {
    return JSON.parse(storage.getUserInfo() || '');
  }
  return '';
};

export default storage;
