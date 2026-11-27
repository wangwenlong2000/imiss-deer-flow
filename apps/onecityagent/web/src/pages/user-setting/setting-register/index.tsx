import { useTranslate } from '@/hooks/common-hooks';
import { useRegister } from '@/hooks/login-hooks';
import { useSaveSetting } from '@/hooks/user-setting-hooks';
import { roleList } from '@/services/user-service';
import { rsaPsw } from '@/utils';
import { Button, Divider, Form, Input, Select, Space, message } from 'antd';
import _ from 'lodash';
import { useEffect, useState } from 'react';
import SettingTitle from '../components/setting-title';
import { useValidateSubmittable } from '../hooks';
import styles from './index.less';

type FieldType = {
  // password?: string;
  new_password?: string;
  confirm_password?: string;
  nickname?: string;
  email?: string;
  chooseRole?: string;
};

const tailLayout = {
  wrapperCol: { offset: 12, span: 4 },
};

const UserSettingPassword = () => {
  const { register, loading: registerLoading } = useRegister();
  const { form, submittable } = useValidateSubmittable();
  const { saveSetting, loading } = useSaveSetting();
  const { t } = useTranslate('setting');

  const handleSubmit = _.debounce(async (values: any) => {
    try {
      const new_password = await rsaPsw(values.new_password);
      await register({
        nickname: values.nickname,
        email: values.email,
        password: new_password,
        role: values.chooseRole,
      });
    } catch {
      message.error(t('registerError'));
    }
  }, 500);
  const onFinish = (values: any) => {
    handleSubmit(values);
  };

  const onFinishFailed = (errorInfo: any) => {
    console.log('Failed:', errorInfo);
  };
  const [roleOptions, setRoleOptions] = useState<
    { value: string; label: string }[]
  >([]); // 初始化状态

  useEffect(() => {
    // 初始化逻辑
    roleList().then((res) => {
      // console.log(getUserInfoObject());
      const options = res?.data?.data.map(
        (role: { value: string; label: string }) => ({
          value: role?.role_id,
          label: t(role?.code),
        }),
      );
      setRoleOptions(options); // 更新状态
    });
    // 如果需要进行异步初始化，可以返回一个清理函数（类似于componentWillUnmount）
    return () => {
      console.log('组件卸载');
    };
  }, []); // 空数组[]意味着effect仅在组件挂载时执行一次

  return (
    <section className={styles.passwordWrapper}>
      <SettingTitle
        title={t('register')}
        description={t('registerDescription')}
      ></SettingTitle>
      <Divider />
      <Form
        colon={false}
        name="basic"
        labelAlign={'left'}
        labelCol={{ span: 4 }}
        wrapperCol={{ span: 10 }}
        style={{ width: '100%' }}
        initialValues={{ remember: true }}
        onFinish={onFinish}
        onFinishFailed={onFinishFailed}
        form={form}
        autoComplete="off"
        // requiredMark={'optional'}
      >
        <Form.Item<FieldType>
          name="email"
          label={t('emailLabel')}
          rules={[
            { required: true, message: t('emailPlaceholder') },
            {
              pattern:
                /^(?=.{1,50}$)[A-Za-z0-9_]+@[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)+$/,
              message: '邮箱格式不正确，长度不超过50个字符',
            },
          ]}
        >
          <Input
            placeholder={t('emailPlaceholder')}
            autoComplete="off"
            // onChange={(e) => {
            //   // 过滤掉空格字符
            //   let value = e.target.value.replace(/\s/g, '');
            //   if (value.length > 50) {
            //     value = value.slice(0, 50);
            //   }
            //   form.setFieldValue('email', value);
            // }}
          />
        </Form.Item>
        <Divider />
        <Form.Item<FieldType>
          name="nickname"
          label="用户名"
          rules={[
            { required: true, message: '请输入用户名' },
            {
              pattern: /^[-_a-zA-Z0-9\u4e00-\u9fa5]{2,10}$/,
              message: '请输入2~10个字符，支持中英文、数字、下划线、连字符',
            },
          ]}
        >
          <Input
            placeholder="请输入用户名"
            // onChange={(e) => {
            //   // 过滤掉空格字符
            //   let value = e.target.value.replace(/\s/g, '');
            //   if (value.length > 10) {
            //     value = value.slice(0, 10);
            //   }
            //   form.setFieldValue('nickname', value);
            // }}
          />
        </Form.Item>
        <Divider />
        {/* <Form.Item label={t('registernewPassword')} required> */}
        <Form.Item label="密码" required>
          <Form.Item<FieldType>
            noStyle
            name="new_password"
            rules={[
              {
                required: true,
                // message: t('newPasswordMessage'),
                message: '请输入密码',
              },

              {
                validator: (_rule: any, value: any) => {
                  if (!value) {
                    return Promise.resolve();
                  }
                  const re =
                    /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@#\u0024])[A-Za-z\d@#\u0024]{8,16}$/;
                  return re.test(value)
                    ? Promise.resolve()
                    : // : Promise.reject(new Error(t('newPasswordDescription')));
                      Promise.reject(
                        new Error(
                          '密码长度8~16位，需包含大小写字母、数字和特殊字符（@、#、$），例如：Abc@12345',
                        ),
                      );
                },
              },
            ]}
          >
            <Input.Password
              // placeholder={t('registernewPasswordMessage')}
              placeholder="请输入密码"
              autoComplete="new-password"
              // onChange={(e) => {
              //   // 过滤掉空格字符
              //   let value = e.target.value.replace(/\s/g, '');
              //   if (value.length > 16) {
              //     value = value.slice(0, 16);
              //   }
              //   form.setFieldValue('new_password', value);
              // }}
            />
          </Form.Item>
        </Form.Item>
        <Divider />
        <Form.Item<FieldType>
          // label={t('registerconfirmPassword')}
          label="确认密码"
          name="confirm_password"
          dependencies={['new_password']}
          rules={[
            {
              required: true,
              message: '请确认密码',
            },
            ({ getFieldValue }) => ({
              validator(_, value) {
                if (!value || getFieldValue('new_password') === value) {
                  return Promise.resolve();
                }
                return Promise.reject(new Error('您输入的密码不匹配！'));
              },
            }),
          ]}
        >
          {/* <Input.Password placeholder={t('registerconfirmPasswordMessage')} /> */}
          <Input.Password
            placeholder="请确认密码"
            // onChange={(e) => {
            //   // 过滤掉空格字符
            //   let value = e.target.value.replace(/\s/g, '');
            //   if (value.length > 16) {
            //     value = value.slice(0, 16);
            //   }
            //   form.setFieldValue('confirm_password', value);
            // }}
          />
        </Form.Item>
        <Divider />
        <Form.Item<FieldType>
          name="chooseRole"
          label={t('chooseRole')}
          rules={[{ required: true, message: t('chooseRolePlaceholder') }]}
        >
          <Select
            placeholder={t('chooseRolePlaceholder')}
            options={roleOptions}
          />
        </Form.Item>
        <Divider />
        <Form.Item
          {...tailLayout}
          shouldUpdate={(prevValues, curValues) =>
            prevValues.additional !== curValues.additional
          }
        >
          <Space style={{ marginBottom: 20 }}>
            {/* <Button htmlType="button">{t('cancel')}</Button> */}
            <Button
              type="primary"
              htmlType="button"
              //disabled={!submittable}
              loading={loading}
              onClick={() => form.submit()}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                }
              }}
            >
              {t('create', { keyPrefix: 'common' })}
            </Button>
          </Space>
        </Form.Item>
      </Form>
    </section>
  );
};

export default UserSettingPassword;
