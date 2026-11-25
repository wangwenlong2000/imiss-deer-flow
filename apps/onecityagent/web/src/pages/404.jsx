import { Button, Result } from 'antd';
import { history } from 'umi';

const NoFoundPage = () => {
  return (
    <Result
      status="404"
      title="404"
      subTitle="找不到页面，请输入正确的地址。"
      extra={
        <Button type="primary" onClick={() => history.push('/')}>
          返回首页
        </Button>
      }
    />
  );
};

export default NoFoundPage;
