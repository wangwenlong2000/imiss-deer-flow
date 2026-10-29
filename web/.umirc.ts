import path from 'path';
import { defineConfig } from 'umi';
import { appName } from './src/conf.json';
import routes from './src/routes';

const backendTarget = process.env.ONECITY_BACKEND_URL;
const coordinatorTarget = process.env.ONECITY_COORDINATOR_URL;

if (!backendTarget || !coordinatorTarget) {
  throw new Error('Set ONECITY_BACKEND_URL and ONECITY_COORDINATOR_URL for the local environment.');
}

export default defineConfig({
  title: appName,
  outputPath: 'dist',
  alias: { '@parent': path.resolve(__dirname, '../') },
  npmClient: 'npm',
  base: '/',
  routes,
  publicPath: '/',
  esbuildMinifyIIFE: true,
  icons: {},
  hash: true,
  favicons: ['/favicon.ico'],
  clickToComponent: {},
  history: {
    type: 'browser',
  },
  plugins: [
    '@react-dev-inspector/umi4-plugin',
    '@umijs/plugins/dist/tailwindcss',
  ],
  jsMinifier: 'terser',
  lessLoader: {
    modifyVars: {
      hack: `true; @import "~@/less/index.less";`,
    },
  },
  devtool: 'source-map',
  copy: ['src/conf.json'],
  proxy: [
    {
      context: ['/api', '/v1'],
      target: backendTarget,
      changeOrigin: true,
      ws: true,
      logger: console, // 移除console logger配置
      // pathRewrite: { '^/v1': '/v1' },
    },
    {
      context: ['/query', '/a2a_reload', '/update_mcp_config'],
      target: coordinatorTarget,
      changeOrigin: true,
      ws: true,
      logger: console,
    },
  ],

  // 插件移除 console
  extraBabelPlugins:
    process.env.NODE_ENV === 'production' ? ['transform-remove-console'] : [],

  chainWebpack(memo, args) {
    memo.module.rule('markdown').test(/\.md$/).type('asset/source');
    return memo;
  },
  tailwindcss: {},
});
