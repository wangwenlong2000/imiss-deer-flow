import { useSaveSetting } from '@/hooks/user-setting-hooks';
import { rsaPsw } from '@/utils';
import { Button, Divider, Form, Input, Space } from 'antd';
import { useCallback, useEffect, useRef, useState } from 'react';
import SettingTitle from '../components/setting-title';
import { useValidateSubmittable } from '../hooks';

import { useTranslate } from '@/hooks/common-hooks';
import authorizationUtil, {
  getAuthorization,
} from '@/utils/authorization-util'; // 请根据实际路径调整导入路径
import { message } from 'antd';
import { history } from 'umi';
import styles from './index.less';

type FieldType = {
  password?: string;
  new_password?: string;
  confirm_password?: string;
};

const tailLayout = {
  wrapperCol: { offset: 12, span: 4 },
};

const UserSettingPassword = () => {
  const { form, submittable } = useValidateSubmittable();
  const { loading: saveLoading } = useSaveSetting();
  const { t } = useTranslate('setting');
  const [localLoading, setLocalLoading] = useState(false); // 本地loading状态
  const timeoutRef = useRef<NodeJS.Timeout | null>(null); // 用于防抖的定时器

  // 防抖处理的提交函数
  const debouncedSubmit = useCallback(
    async (values: any) => {
      // 立即设置loading状态
      setLocalLoading(true);

      const password = rsaPsw(values.password) as string;
      const new_password = rsaPsw(values.new_password) as string;

      try {
        // 先获取认证信息
        const currentAuth = getAuthorization();

        // 添加请求超时处理
        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(() => reject(new Error('请求超时')), 30000); // 30秒超时
        });

        const fetchPromise = fetch('/v1/user/setting', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: currentAuth,
          },
          body: JSON.stringify({ password, new_password }),
        });

        // 使用Promise.race处理超时情况
        const response = (await Promise.race([
          fetchPromise,
          timeoutPromise,
        ])) as Response;

        const data = await response.json();
        if (data.code === 0) {
          message.success('密码修改成功');
          form.resetFields();
          authorizationUtil.removeAll();
          history.push('/login');
        } else {
          message.error('密码修改失败，请检查当前密码是否正确', 3);
        }
      } catch (err) {
        message.error('密码修改失败，请稍后重试', 3);
      } finally {
        // 无论成功失败或超时，都确保重置loading状态
        setLocalLoading(false);
      }
    },
    [form],
  );

  // 带有防抖逻辑的onFinish
  const onFinish = useCallback(
    (values: any) => {
      // 如果已经有定时器在运行，清除它（取消上一次的调用）
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }

      // 设置新的定时器，延迟执行提交操作（防抖时间设为300ms）
      timeoutRef.current = setTimeout(() => {
        debouncedSubmit(values);
      }, 300);
    },
    [debouncedSubmit],
  );

  // 组件卸载时清理定时器
  useEffect(() => {
    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, []);

  const onFinishFailed = (errorInfo: any) => {
    console.log('Failed:', errorInfo);
  };

  return (
    <section className={styles.passwordWrapper}>
      <SettingTitle
        // title={t('password')}
        title="密码"
        // description={t('passwordDescription')}
        description="请输入您当前的密码以更改您的密码。"
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
          // label={t('currentPassword')}
          label="当前密码"
          name="password"
          rules={[
            {
              required: true,
              // message: t('currentPasswordMessage'),
              message: '请输入当前密码',
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
            placeholder="请输入当前密码"
            // onChange={(e) => {
            //   // 过滤掉空格字符并限制长度不超过16位
            //   let value = e.target.value.replace(/\s/g, '');
            //   // 限制最大长度为16位
            //   if (value.length > 16) {
            //     value = value.slice(0, 16);
            //   }
            //   form.setFieldValue('password', value);
            // }}
          />
        </Form.Item>
        <Divider />
        {/* <Form.Item label={t('newPassword')} required> */}
        <Form.Item label="新密码" required>
          <Form.Item<FieldType>
            noStyle
            name="new_password"
            dependencies={['password']}
            rules={[
              {
                required: true,
                // message: t('newPasswordMessage'),
                message: '请输入新密码',
              },
              {
                validator: (_rule: any, value: any) => {
                  if (!value) {
                    return Promise.resolve();
                  }
                  // 校验：长度为8到20位，必须包含数字、小写和大写字母；且至少包含 @、# 或 $ 中的一个
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
              ({ getFieldValue }) => ({
                validator(_, value) {
                  if (!value) {
                    return Promise.resolve();
                  }
                  if (getFieldValue('password') === value) {
                    return Promise.reject(
                      new Error('新密码不能与当前密码相同'),
                    );
                  }
                  return Promise.resolve();
                },
              }),
            ]}
          >
            <Input.Password
              placeholder="请输入新密码"
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
          // label={t('confirmPassword')}
          label="确认新密码"
          name="confirm_password"
          dependencies={['new_password']}
          rules={[
            {
              required: true,
              // message: t('confirmPasswordMessage'),
              message: '请确认新密码',
            },
            ({ getFieldValue }) => ({
              validator(_, value) {
                if (!value || getFieldValue('new_password') === value) {
                  return Promise.resolve();
                }
                return Promise.reject(
                  // new Error(t('confirmPasswordNonMatchMessage')),
                  new Error('您输入的新密码不匹配！'),
                );
              },
            }),
          ]}
        >
          <Input.Password
            placeholder="请确认新密码"
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
              disabled={!submittable || localLoading}
              loading={localLoading || saveLoading}
              onClick={() => form.submit()}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                }
              }}
            >
              {t('save', { keyPrefix: 'common' })}
            </Button>
          </Space>
        </Form.Item>
      </Form>
    </section>
  );
};

export default UserSettingPassword;
