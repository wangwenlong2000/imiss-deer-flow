import { ReactComponent as LockOn } from '@/assets/picture/lock-on.svg';
import { ReactComponent as Mail } from '@/assets/picture/mail.svg';
import { ReactComponent as ChinaMobile } from '@/assets/picture/mobile.svg';
import { useLogin, useRegister } from '@/hooks/login-hooks';
import { rsaPsw } from '@/utils';
import { Button, Form, Input, message } from 'antd';
import _ from 'lodash';
import { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'umi';

import styles from './index.less';

const Login = () => {
  const [title, setTitle] = useState('login');
  const navigate = useNavigate();
  const { login, loading: signLoading } = useLogin();
  const { register, loading: registerLoading } = useRegister();
  const { t } = useTranslation('translation', { keyPrefix: 'login' });
  const loading = signLoading || registerLoading;

  // const changeTitle = () => {
  //   setTitle((title) => (title === 'login' ? 'register' : 'login'));
  // };
  const [form] = Form.useForm();

  // useEffect(() => {
  //   form.validateFields(['nickname']);
  // }, [form]);

  // 加载保存的登录信息
  useEffect(() => {
    // form.setFieldsValue({
    //   email: '',
    //   password: '',
    //   remember: '',
    // });
    // if (title === 'login') {
    //   const savedEmail = localStorage.getItem('rememberedEmail');
    //   const savedPassword = localStorage.getItem('rememberedPassword');
    //   const savedRemember = localStorage.getItem('remembered') === 'true';
    //   if (savedEmail) {
    //     form.setFieldsValue({
    //       email: savedEmail,
    //       password: savedPassword || '',
    //       remember: savedRemember,
    //     });
    //   }
    // }
  }, [form]);

  const onCheck = async () => {
    try {
      const params = await form.validateFields();

      const rsaPassWord = await rsaPsw(params.password);

      if (title === 'login') {
        const code = await login({
          email: `${params.email}`.trim(),
          password: rsaPassWord,
        });
        if (code === 0) {
          // 保存登录信息到本地存储
          if (params.remember) {
            // localStorage.setItem('rememberedEmail', `${params.email}`.trim());
            // localStorage.setItem('rememberedPassword', params.password); // 保存密码
            // localStorage.setItem('remembered', 'true');
            localStorage.removeItem('rememberedEmail');
            localStorage.removeItem('rememberedPassword');
            localStorage.removeItem('remembered');
          } else {
            localStorage.removeItem('rememberedEmail');
            localStorage.removeItem('rememberedPassword');
            localStorage.removeItem('remembered');
          }
          navigate('/chat');
        }
      } else {
        const code = await register({
          nickname: params.nickname,
          email: params.email,
          password: rsaPassWord,
          role: 'user',
        });
        if (code === 0) {
          setTitle('login');
        }
      }
    } catch (errorInfo) {
      if (errorInfo instanceof Error) {
        message.error(errorInfo.message || '登录请求失败，请稍后重试');
      }
    }
  };
  const formItemLayout = {
    labelCol: { span: 6 },
    // wrapperCol: { span: 8 },
  };

  const debouncedOnCheck = useMemo(
    () => _.debounce(onCheck, 500, { leading: true, trailing: false }),
    [form, title, login, register, navigate],
  );

  return (
    <div className={styles.loginPage}>
      <div className={styles.loginLeft}>
        <div className={styles.leftContainer}>
          <div className={styles.loginTitle}>
            <div>欢迎登录城市治理多智能体协同引擎</div>
            {/* <span>
              {title === 'login'
                ? t('loginDescription')
                : t('registerDescription')}
            </span> */}
          </div>

          <Form form={form} layout="vertical" name="dynamic_rule">
            <Form.Item
              {...formItemLayout}
              name="email"
              // label={t('emailLabel')}
              rules={[
                { required: true, message: t('emailPlaceholder') },
                title === 'login' ? {
                  type: 'email',
                  transform: (value: string) => value?.trim(),
                  message: '邮箱格式不正确',
                } : {
                  pattern:
                    /^(?=.{1,50}$)[A-Za-z0-9_]+@[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)+$/,
                  message: '邮箱格式不正确',
                },
              ]}
              className={styles.loginInput}
            >
              <Input
                size="large"
                placeholder={t('emailPlaceholder')}
                prefix={<Mail />}
                style={{ height: '0.4rem', fontSize: '0.14rem' }}
                autoComplete="new-email"
              />
            </Form.Item>
            {/* {title === 'register' && (
              <Form.Item
                {...formItemLayout}
                name="nickname"
                label={t('nicknameLabel')}
                rules={[{ required: true, message: t('nicknamePlaceholder') }]}
              >
                <Input size="large" placeholder={t('nicknamePlaceholder')} />
              </Form.Item>
            )} */}
            <Form.Item
              {...formItemLayout}
              name="password"
              // label={t('passwordLabel')}
              rules={[
                // { required: true, message: t('passwordPlaceholder') },
                { required: true, message: '请输入密码' },

                {
                  validator: (_rule: any, value: any) => {
                    if (!value || title === 'login') {
                      return Promise.resolve();
                    }
                    const re =
                      /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@#\u0024])[A-Za-z\d@#\u0024]{8,16}$/;
                    return re.test(value)
                      ? Promise.resolve()
                      : // : Promise.reject(new Error(t('newPasswordDescription')));
                        Promise.reject(new Error('密码格式不正确'));
                  },
                },
              ]}
              className={styles.loginInput}
            >
              <Input.Password
                size="large"
                // placeholder={t('passwordPlaceholder')}
                placeholder="请输入密码"
                style={{ height: '0.4rem', fontSize: '0.14rem' }}
                prefix={<LockOn />}
                autoComplete="new-password"
              />
            </Form.Item>

            {/* <div>
              {title === 'login' && (
                <div>
                  {t('signInTip')}
                  <Button type="link" onClick={changeTitle}>
                    {t('signUp')}
                  </Button>
                </div>
              )}
              {title === 'register' && (
                <div>
                  {t('signUpTip')}
                  <Button type="link" onClick={changeTitle}>
                    {t('login')}
                  </Button>
                </div>
              )}
            </div> */}
            <Button
              type="primary"
              block
              size="large"
              onClick={debouncedOnCheck}
              loading={loading}
              className={styles.loginButton}
            >
              {title === 'login' ? t('login') : t('continue')}
            </Button>
            {title === 'login' && (
              <Form.Item
                name="remember"
                valuePropName="checked"
                className={styles.loginFormItem}
              >
                {/* <Checkbox> {t('rememberMe')}</Checkbox> */}
              </Form.Item>
            )}
            {/* {title === 'login' && (
              <>
                <Button
                  block
                  size="large"
                  onClick={toGoogle}
                  style={{ marginTop: 15 }}
                >
                  <div>
                    <Icon
                      icon="local:google"
                      style={{ verticalAlign: 'middle', marginRight: 5 }}
                    />
                    Sign in with Google
                  </div>
                </Button> 
                {location.host === Domain && (
                  <Button
                    block
                    size="large"
                    onClick={toGoogle}
                    style={{ marginTop: 15 }}
                  >
                    <div>
                      <Icon
                        icon="local:github"
                        style={{ verticalAlign: 'middle', marginRight: 5 }}
                      />
                      Sign in with Github
                    </div>
                  </Button>
                )}
              </>
            )} */}
          </Form>
        </div>
      </div>
      <div className={styles.loginRight}></div>
      <div className={styles.loginTop}>
        <ChinaMobile></ChinaMobile>
        <div style={{ marginLeft: '0.08rem' }}>城市治理多智能体协同引擎</div>
      </div>
      <div className={styles.copyright}>
        Copyright ©2005 - {new Date().getFullYear()} 中移系统集成有限公司 All
        Rights Reserved.
      </div>
    </div>
  );
};

export default Login;
