import { Authorization } from '@/constants/authorization';
import { ResponseType } from '@/interfaces/database/base';
import i18n from '@/locales/config';
import authorizationUtil, {
  getAuthorization,
} from '@/utils/authorization-util';
import { message, notification } from 'antd';
import { history } from 'umi';
import { RequestMethod, extend } from 'umi-request';
import { convertTheKeysOfTheObjectToSnake } from './common-util';

const FAILED_TO_FETCH = 'Failed to fetch';

const RetcodeMessage = {
  200: i18n.t('message.200'),
  201: i18n.t('message.201'),
  202: i18n.t('message.202'),
  204: i18n.t('message.204'),
  400: i18n.t('message.400'),
  401: i18n.t('message.401'),
  403: i18n.t('message.403'),
  404: i18n.t('message.404'),
  406: i18n.t('message.406'),
  410: i18n.t('message.410'),
  413: i18n.t('message.413'),
  422: i18n.t('message.422'),
  500: i18n.t('message.500'),
  502: i18n.t('message.502'),
  503: i18n.t('message.503'),
  504: i18n.t('message.504'),
};
type ResultCode =
  | 200
  | 201
  | 202
  | 204
  | 400
  | 401
  | 403
  | 404
  | 406
  | 410
  | 413
  | 422
  | 500
  | 502
  | 503
  | 504;

const errorHandler = (error: {
  response: Response;
  message: string;
}): Response => {
  const { response } = error;
  if (error.message === FAILED_TO_FETCH) {
    notification.error({
      description: '您的网络发生异常，无法连接服务器',
      // message: i18n.t('message.networkAnomaly'),
      //message: '接口请求错误',
      duration: 3,
      closeIcon: null,
    });
  }
  // else {
  //   if (response && response.status) {
  //     const errorText =
  //       RetcodeMessage[response.status as ResultCode] || response.statusText;
  //     const { status, url } = response;
  //     notification.error({
  //       message: `${i18n.t('message.requestError')} ${status}: ${url}`,
  //       description: errorText,
  //       duration: 3,
  //     });
  //   }
  // }
  return response;
};

const request: RequestMethod = extend({
  errorHandler,
  timeout: 300000,
  getResponse: true,
});

request.interceptors.request.use((url: string, options: any) => {
  const data = convertTheKeysOfTheObjectToSnake(options.data);
  const params = convertTheKeysOfTheObjectToSnake(options.params);

  return {
    url,
    options: {
      ...options,
      data,
      params,
      headers: {
        ...(options.skipToken
          ? undefined
          : { [Authorization]: getAuthorization() }),
        ...options.headers,
      },
      interceptors: true,
    },
  };
});

request.interceptors.response.use(async (response: any, options) => {
  if (response?.status === 413 || response?.status === 504) {
    message.error(RetcodeMessage[response?.status as ResultCode]);
  }

  if (options.responseType === 'blob') {
    return response;
  }

  const data: ResponseType = await response.clone().json();

  if (data.code === 401 || data.code === 403) {
    if (data.message !== '请提供邮箱和密码进行登录') {
      if (data.message === '邮箱或密码不正确') {
        notification.error({
          // message: '登录错误',
          description: data.message,
          duration: 3,
          closeIcon: null,
        });
        return;
      }
      notification.error({
        // message: '接口请求错误',
        description: data.message,
        duration: 3,
        closeIcon: null,
      });
    }
    authorizationUtil.removeAll();
    history.push('/login'); // Will not jump to the login page
  } else if (data.code !== 0) {
    if (data.code === 100) {
      message.error(data.message);
    } else if (data.code === 500) {
      // notification.error({
      //   message: '系统错误,请联系管理员',
      //   description: data.message,
      //   duration: 3,
      // });
    } else {
      notification.error({
        // message: '接口请求错误',
        description: data.message,
        duration: 3,
        closeIcon: null,
      });
    }

    return response;
  } else {
    return response;
  }
});

export default request;

export const get = (url: string) => {
  return request.get(url);
};

export const post = (url: string, body: any) => {
  return request.post(url, { data: body });
};

export const drop = () => {};

export const put = () => {};
