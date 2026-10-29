import { useFetchUserInfo, useSaveSetting } from '@/hooks/user-setting-hooks';
// import {
//   getBase64FromUploadFileList,
//   getUploadFileListFromBase64,
// } from '@/utils/file-util';
import {
  Button,
  Divider,
  Form,
  Input,
  // Select,
  Space,
  Spin,
} from 'antd';
import _ from 'lodash';
import { useEffect } from 'react';
import SettingTitle from '../components/setting-title';
import { useValidateSubmittable } from '../hooks';

import { useTranslate } from '@/hooks/common-hooks';
import parentStyles from '../index.less';
import styles from './index.less';

// const { Option } = Select;

type FieldType = {
  nickname?: string;
  email?: string;
  color_schema?: string;
  timezone?: string;
  // avatar?: string;
};

const tailLayout = {
  wrapperCol: { offset: 12, span: 4 },
};

const UserSettingProfile = () => {
  const { data: userInfo, loading } = useFetchUserInfo();
  const { saveSetting, loading: submitLoading } = useSaveSetting();
  const { form, submittable } = useValidateSubmittable();
  const { t } = useTranslate('setting');

  const onFinish = _.debounce(async (values: any) => {
    // const avatar = await getBase64FromUploadFileList(values.avatar);
    saveSetting({ ...values });
  }, 500); // 500毫秒防抖延迟，连续点击只触发最后一次

  const onFinishFailed = (errorInfo: any) => {
    console.log('Failed:', errorInfo);
  };

  // const beforeUpload = (file: UploadFile) => {
  //   const isImage = file.type ? file.type.startsWith('image/') : false;
  //   if (!isImage) {
  //     message.error('只能上传图片文件');
  //     return isImage || Upload.LIST_IGNORE;
  //   }
  //   // 限制特定图片格式
  //   const allowedExts = ['jpg', 'jpeg', 'gif', 'png'];
  //   const name = (file.name || '').toString();
  //   const ext = name.includes('.') ? name.split('.').pop()!.toLowerCase() : '';
  //   if (!allowedExts.includes(ext)) {
  //     message.error('只支持 jpg、jpeg、gif、png 格式的图片');
  //     return Upload.LIST_IGNORE;
  //   }
  //   const isLt1M = file.size ? file.size / 1024 / 1024 < 1 : false;
  //   if (!isLt1M) {
  //     message.error('图片大小不能超过1MB');
  //     return Upload.LIST_IGNORE;
  //   }

  //   return false;
  // };

  useEffect(() => {
    // const fileList: UploadFile[] = getUploadFileListFromBase64(userInfo.avatar);
    form.setFieldsValue({ ...userInfo });
  }, [form, userInfo]);

  return (
    <section className={styles.profileWrapper}>
      <SettingTitle
        title={t('profile')}
        description={t('profileDescription')}
      ></SettingTitle>
      <Divider />
      <Spin spinning={loading}>
        <Form
          colon={false}
          name="basic"
          labelAlign={'left'}
          labelCol={{ span: 4 }}
          wrapperCol={{ span: 10 }}
          labelWrap
          style={{ width: '100%' }}
          initialValues={{ remember: true }}
          onFinish={onFinish}
          onFinishFailed={onFinishFailed}
          form={form}
          autoComplete="off"
        >
          <Form.Item<FieldType>
            label={t('username')}
            name="nickname"
            rules={[
              {
                required: true,
                message: t('usernameMessage'),
              },
              {
                pattern: /^[-_a-zA-Z0-9\u4e00-\u9fa5]{2,10}$/,
                message: '请输入2~10个字符，支持中英文、数字、下划线、连字符',
              },
            ]}
          >
            <Input
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
          {/* <Form.Item<FieldType>
            label={
              <div>
                <Space>{t('photo')}</Space>
                <div>{t('photoDescription')}</div>
              </div>
            }
            name="avatar"
            valuePropName="fileList"
            getValueFromEvent={normFile}
            extra={
              <span style={{ color: 'rgba(0, 0, 0, 0.45)' }}>
                支持 jpg、jpeg、gif、png 格式，大小不能超过 1MB 的图片
              </span>
            }
          >
            <Upload
              listType="picture-card"
              maxCount={1}
              accept=".jpg,.jpeg,.gif,.png"
              beforeUpload={beforeUpload}
              fileList={form.getFieldValue('avatar') || []}
              showUploadList={{ showPreviewIcon: false, showRemoveIcon: false }}
            >
              <button style={{ border: 0, background: 'none' }} type="button">
                <PlusOutlined />
                <div style={{ marginTop: 8 }}>
                  {t('upload', { keyPrefix: 'common' })}
                </div>
              </button>
            </Upload>
          </Form.Item>
          <Divider /> */}
          {/* <Form.Item<FieldType>
            label={t('colorSchema')}
            name="color_schema"
            rules={[{ required: true, message: t('colorSchemaMessage') }]}
          >
            <Select placeholder={t('colorSchemaPlaceholder')}>
              <Option value="Bright">{t('bright')}</Option>
              <Option value="Dark">{t('dark')}</Option>
            </Select>
          </Form.Item>
          <Divider /> */}
          {/* <Form.Item<FieldType>
            label={t('language', { keyPrefix: 'common' })}
            name="language"
            rules={[
              {
                required: true,
                message: t('languageMessage', { keyPrefix: 'common' }),
              },
            ]}
          >
            <Select
              placeholder={t('languagePlaceholder', { keyPrefix: 'common' })}
              onChange={changeLanguage}
            >
              {LanguageList.map((x) => (
                <Option value={x} key={x}>
                  {t(camelCase(x), { keyPrefix: 'common' })}
                </Option>
              ))}
            </Select>
          </Form.Item>
          <Divider /> */}
          {/* <Form.Item<FieldType>
            label={t('timezone')}
            name="timezone"
            rules={[{ required: true, message: t('timezoneMessage') }]}
          >
            <Select placeholder={t('timezonePlaceholder')} showSearch>
              {TimezoneList.map((x) => (
                <Option value={x.en} key={x.en}>
                  {x.zh}
                </Option>
              ))}
            </Select>
          </Form.Item>
          <Divider /> */}
          <Form.Item label={t('email')}>
            <Form.Item<FieldType> name="email" noStyle>
              <Input disabled />
            </Form.Item>
            <p className={parentStyles.itemDescription}>
              {t('emailDescription')}
            </p>
          </Form.Item>
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
                disabled={!submittable}
                loading={submitLoading}
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
      </Spin>
    </section>
  );
};

export default UserSettingProfile;
