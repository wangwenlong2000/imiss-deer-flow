import { PlusOutlined } from '@ant-design/icons';
import { Col, Form, Input, Row, Switch, Upload, message } from 'antd';
import classNames from 'classnames';
import { ISegmentedContentProps } from '../interface';

import KnowledgeBaseItem from '@/components/knowledge-base-item';
import { useTranslate } from '@/hooks/common-hooks';
import { useFetchTenantInfo } from '@/hooks/user-setting-hooks';
import { useCallback } from 'react';
import styles from './index.less';

const AssistantSetting = ({ show, form }: ISegmentedContentProps) => {
  const { t } = useTranslate('chat');
  const { data } = useFetchTenantInfo();

  const normFile = (e: any) => {
    if (Array.isArray(e)) {
      return e;
    }
    return e?.fileList;
  };

  const handleTtsChange = useCallback(
    (checked: boolean) => {
      if (checked && !data.tts_id) {
        message.error(`Please set TTS model firstly. 
        Setting >> Model Providers >> System model settings`);
        form.setFieldValue(['prompt_config', 'tts'], false);
      }
    },
    [data, form],
  );

  const uploadButtion = (
    <button style={{ border: 0, background: 'none' }} type="button">
      <PlusOutlined />
      <div style={{ marginTop: 8 }}>{t('upload', { keyPrefix: 'common' })}</div>
    </button>
  );

  return (
    <section
      className={classNames({
        [styles.segmentedHidden]: !show,
      })}
    >
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            name="icon"
            label={t('assistantAvatar')}
            valuePropName="fileList"
            getValueFromEvent={normFile}
          >
            <Upload
              listType="picture-card"
              maxCount={1}
              beforeUpload={() => false}
              showUploadList={{ showPreviewIcon: false, showRemoveIcon: false }}
            >
              {show ? uploadButtion : null}
            </Upload>
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item
            name={['prompt_config', 'prologue']}
            label={t('setAnOpener')}
            tooltip={t('setAnOpenerTip')}
            initialValue={t('setAnOpenerInitial')}
          >
            <Input.TextArea autoSize={{ minRows: 5 }} />
          </Form.Item>
        </Col>
      </Row>
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            name={'name'}
            label={t('assistantName')}
            rules={[{ required: true, message: t('assistantNameMessage') }]}
          >
            <Input placeholder={t('namePlaceholder')} />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item name={'description'} label={t('description')}>
            <Input placeholder={t('descriptionPlaceholder')} />
          </Form.Item>
        </Col>
      </Row>

      {/* <Form.Item
        name={'language'}
        label={t('language')}
        initialValue={'English'}
        tooltip="coming soon"
        style={{ display: 'none' }}
      >
        <Select
          options={[
            { value: 'Chinese', label: t('chinese', { keyPrefix: 'common' }) },
            { value: 'English', label: t('english', { keyPrefix: 'common' }) },
          ]}
        />
      </Form.Item> */}
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            name={['prompt_config', 'empty_response']}
            label={t('emptyResponse')}
            tooltip={t('emptyResponseTip')}
          >
            <Input placeholder="" />
          </Form.Item>
        </Col>
        <Col span={12}>
          <KnowledgeBaseItem></KnowledgeBaseItem>
        </Col>
      </Row>

      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            label={t('quote')}
            valuePropName="checked"
            name={['prompt_config', 'quote']}
            tooltip={t('quoteTip')}
            initialValue={true}
          >
            <Switch />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item
            label={t('tts')}
            valuePropName="checked"
            name={['prompt_config', 'tts']}
            tooltip={t('ttsTip')}
            initialValue={false}
          >
            <Switch onChange={handleTtsChange} />
          </Form.Item>
        </Col>
      </Row>

      {/* <Form.Item
        label={t('selfRag')}
        valuePropName="checked"
        name={['prompt_config', 'self_rag']}
        tooltip={t('selfRagTip')}
        initialValue={false}
      >
        <Switch />
      </Form.Item> */}
    </section>
  );
};

export default AssistantSetting;
