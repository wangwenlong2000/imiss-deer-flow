import api from '@/utils/api';
import registerServer from '@/utils/register-server';
import request from '@/utils/request';

const {
  getDialog,
  getChatList,
  setDialog,
  createDialog,
  listDialog,
  removeDialog,
  updateDialog,
  getConversation,
  setConversation,
  completeConversation,
  listConversation,
  removeConversation,
  createToken,
  listToken,
  removeToken,
  getStats,
  createExternalConversation,
  getExternalConversation,
  completeExternalConversation,
  uploadAndParseExternal,
  deleteMessage,
  thumbup,
  tts,
  ask,
  mindmap,
  getRelatedQuestions,
} = api;

const methods = {
  getDialog: {
    url: getDialog,
    method: 'get',
  },
  setDialog: {
    url: setDialog,
    method: 'post',
  },
  removeDialog: {
    url: removeDialog,
    method: 'delete',
  },
  listDialog: {
    url: listDialog,
    method: 'get',
  },
  listConversation: {
    url: listConversation,
    method: 'get',
  },
  getConversation: {
    url: getConversation,
    method: 'get',
  },
  setConversation: {
    url: setConversation,
    method: 'post',
  },
  completeConversation: {
    url: completeConversation,
    method: 'post',
  },
  removeConversation: {
    url: removeConversation,
    method: 'post',
  },
  createToken: {
    url: createToken,
    method: 'post',
  },
  listToken: {
    url: listToken,
    method: 'get',
  },
  removeToken: {
    url: removeToken,
    method: 'post',
  },
  getStats: {
    url: getStats,
    method: 'get',
  },
  createExternalConversation: {
    url: createExternalConversation,
    method: 'get',
  },
  getExternalConversation: {
    url: getExternalConversation,
    method: 'get',
  },
  completeExternalConversation: {
    url: completeExternalConversation,
    method: 'post',
  },
  uploadAndParseExternal: {
    url: uploadAndParseExternal,
    method: 'post',
  },
  deleteMessage: {
    url: deleteMessage,
    method: 'post',
  },
  thumbup: {
    url: thumbup,
    method: 'post',
  },
  tts: {
    url: tts,
    method: 'post',
  },
  ask: {
    url: ask,
    method: 'post',
  },
  getMindMap: {
    url: mindmap,
    method: 'post',
  },
  getRelatedQuestions: {
    url: getRelatedQuestions,
    method: 'post',
  },
  createDialog: {
    url: createDialog,
    method: 'post',
  },
  getChatList: {
    url: getChatList,
    method: 'get',
  },
} as const;

export const paramsMethod = {
  removeDialog(dialogId: string) {
    return request.delete(`${removeDialog}/${dialogId}`);
  },

  updateConfig(dialogId: string, data: any) {
    return request.put(`${updateDialog}/${dialogId}`, {
      data: data,
    });
  },

  finishedConversation(dialogId: string) {
    return request.put(`/v1/dialog/finish/${dialogId}`);
  },
};

const chatService = registerServer<keyof typeof methods>(methods, request);

export default chatService;
