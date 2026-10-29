const routes = [
  {
    path: '/login',
    component: '@/pages/login',
    layout: false,
  },
  // {
  //   path: '/login-next',
  //   component: '@/pages/login-next',
  //   layout: false,
  // },
  // {
  //   path: '/chat/share',
  //   component: '@/pages/chat/share',
  //   layout: false,
  // },
  {
    path: '/',
    component: '@/layouts',
    layout: false,
    wrappers: ['@/wrappers/auth'],
    routes: [
      { path: '/', redirect: '/chat' },
      {
        path: '/chat',
        component: '@/pages/chat',
      },
      {
        path: '/city-brain',
        component: '@/pages/city-brain',
      },
      {
        path: '/schemes',
        component: '@/pages/schemas',
      },
      {
        path: '/user-setting',
        component: '@/pages/user-setting',
        routes: [
          { path: '/user-setting', redirect: '/user-setting/profile' },
          {
            path: '/user-setting/profile',
            component: '@/pages/user-setting/setting-profile',
          },
          {
            path: '/user-setting/password',
            component: '@/pages/user-setting/setting-password',
          },
          // {
          //   path: '/user-setting/model',
          //   component: '@/pages/user-setting/setting-model',
          // },
          // {
          //   path: '/user-setting/team',
          //   component: '@/pages/user-setting/setting-team',
          // },
          // {
          //   path: '/user-setting/system',
          //   component: '@/pages/user-setting/setting-system',
          // },
          // {
          //   path: '/user-setting/api',
          //   component: '@/pages/user-setting/setting-api',
          // },
          {
            path: '/user-setting/register',
            component: '@/pages/user-setting/setting-register',
          },
          {
            path: '/user-setting/mcp',
            component: '@/pages/user-setting/setting-mcp',
          },
        ],
      },
      // {
      //   path: '/file',
      //   component: '@/pages/file-manager',
      // },
      // 流程界面已被屏蔽
      // {
      //   path: '/flow',
      //   component: '@/pages/flow/list',
      // },
      // {
      //   path: '/flow/:id',
      //   component: '@/pages/flow',
      // },
      // {
      //   path: '/search',
      //   component: '@/pages/search',
      // },
    ],
  },
  // {
  //   path: '/document/:id',
  //   component: '@/pages/document-viewer',
  //   layout: false,
  // },
  // {
  //   path: 'force',
  //   component: '@/pages/force-graph',
  //   layout: false,
  // },
  {
    path: '/*',
    component: '@/pages/404',
    layout: false,
  },
  // {
  //   path: '/demo',
  //   component: '@/pages/demo',
  //   layout: false,
  // },
  // {
  //   path: '/home',
  //   layout: false,
  //   component: '@/pages/home',
  // },
  // {
  //   path: '/datasets',
  //   layout: false,
  //   component: '@/layouts/next',
  //   routes: [
  //     {
  //       path: '/datasets',
  //       component: '@/pages/datasets',
  //     },
  //   ],
  // },
  // {
  //   path: '/dataset',
  //   layout: false,
  //   component: '@/layouts/next',
  //   routes: [
  //     { path: '/dataset', redirect: '/dataset/dataset' },
  //     {
  //       path: '/dataset',
  //       component: '@/pages/dataset',
  //       routes: [
  //         {
  //           path: '/dataset/dataset',
  //           component: '@/pages/dataset/dataset',
  //         },
  //         {
  //           path: '/dataset/configuration',
  //           component: '@/pages/dataset/settings',
  //         },
  //         {
  //           path: '/dataset/testing',
  //           component: '@/pages/dataset/testing',
  //         },
  //       ],
  //     },
  //   ],
  // },
  // {
  //   path: '/profile-setting',
  //   layout: false,
  //   component: '@/pages/profile-setting',
  //   routes: [
  //     { path: '/profile-setting', redirect: '/profile-setting/profile' },
  //     {
  //       path: '/profile-setting/profile',
  //       component: '@/pages/profile-setting/profile',
  //     },
  //     {
  //       path: '/profile-setting/team',
  //       component: '@/pages/profile-setting/team',
  //     },
  //     {
  //       path: '/profile-setting/plan',
  //       component: '@/pages/profile-setting/plan',
  //     },
  //     {
  //       path: '/profile-setting/model',
  //       component: '@/pages/profile-setting/model',
  //     },
  //     {
  //       path: '/profile-setting/prompt',
  //       component: '@/pages/profile-setting/prompt',
  //     },
  //   ],
  // },
];

export default routes;
