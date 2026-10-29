import {
  AutoKeywordsItem,
  AutoQuestionsItem,
} from '@/components/auto-keywords-item';
import { useShowAutoKeywords } from '@/components/chunk-method-modal/hooks';
import Delimiter from '@/components/delimiter';
import EntityTypesItem from '@/components/entity-types-item';
import ExcelToHtml from '@/components/excel-to-html';
import LayoutRecognize from '@/components/layout-recognize';
import MaxTokenNumber from '@/components/max-token-number';
import ParseConfiguration, {
  showRaptorParseConfiguration,
} from '@/components/parse-configuration';
import { useTranslate } from '@/hooks/common-hooks';
import { useHandleChunkMethodSelectChange } from '@/hooks/logic-hooks';
import { normFile } from '@/utils/file-util';
import { PlusOutlined } from '@ant-design/icons';
import {
  Button,
  Col,
  Form,
  Input,
  Radio,
  Row,
  Select,
  Space,
  Upload,
} from 'antd';
import { FormInstance } from 'antd/lib';
import camelCase from 'lodash/camelCase';
import { useMemo } from 'react';
import {
  useFetchKnowledgeConfigurationOnMount,
  useSubmitKnowledgeConfiguration,
} from './hooks';
import styles from './index.less';
import './style.css';

const { Option } = Select;

const ConfigurationForm = ({
  form,
  chunkMethod,
}: {
  form: FormInstance;
  chunkMethod: string;
}) => {
  const { submitKnowledgeConfiguration, submitLoading, navigateToDataset } =
    useSubmitKnowledgeConfiguration(form);
  const { parserList, embeddingModelOptions, disabled } =
    useFetchKnowledgeConfigurationOnMount(form);
  const { t } = useTranslate('knowledgeConfiguration');
  const handleChunkMethodSelectChange = useHandleChunkMethodSelectChange(form);
  const showAutoKeywords = useShowAutoKeywords();
  const item = useMemo(() => {
    const item = parserList.find((x) => x.value === chunkMethod);
    if (item) {
      return {
        title: item.label,
        description: t(camelCase(item.value)),
      };
    }
    return { title: '', description: '' };
  }, [parserList, chunkMethod, t]);

  return (
    <Form form={form} name="validateOnly" layout="vertical" autoComplete="off">
      <Form.Item
        name="avatar"
        label={t('photo')}
        valuePropName="fileList"
        getValueFromEvent={normFile}
      >
        <Upload
          action="/upload.do"
          listType="picture-card"
          maxCount={1}
          beforeUpload={() => false}
          showUploadList={{ showPreviewIcon: false, showRemoveIcon: false }}
        >
          <button style={{ border: 0, background: 'none' }} type="button">
            <PlusOutlined />
            <div style={{ marginTop: 8 }}>{t('upload')}</div>
          </button>
        </Upload>
      </Form.Item>
      <Row gutter={32}>
        <Col span={12}>
          <Form.Item name="name" label={t('name')} rules={[{ required: true }]}>
            <Input />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item name="description" label={t('description')}>
            <Input />
          </Form.Item>
        </Col>
      </Row>

      <Row gutter={32}>
        <Col span={12}>
          <Form.Item
            label={t('language')}
            name="language"
            initialValue={'Chinese'}
            rules={[{ required: true, message: t('languageMessage') }]}
          >
            <Select placeholder={t('languagePlaceholder')}>
              <Option value="Chinese">{t('chinese')}</Option>
              <Option value="English">{t('english')}</Option>
            </Select>
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item
            name="permission"
            label={t('permissions')}
            tooltip={t('permissionsTip')}
            rules={[{ required: true }]}
          >
            <Radio.Group>
              <Radio value="me">{t('me')}</Radio>
              <Radio value="team">{t('team')}</Radio>
            </Radio.Group>
          </Form.Item>
        </Col>
      </Row>
      <Row gutter={32}>
        <Col span={12}>
          <Form.Item
            name="embd_id"
            label={t('embeddingModel')}
            rules={[{ required: true }]}
            tooltip={t('embeddingModelTip')}
          >
            <Select
              placeholder={t('embeddingModelPlaceholder')}
              options={embeddingModelOptions}
              disabled={disabled}
            ></Select>
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item
            name="parser_id"
            label={t('chunkMethod')}
            tooltip={item.description.replace(/<\/?(p|b)>/g, '')}
            rules={[{ required: true }]}
          >
            <Select
              placeholder={t('chunkMethodPlaceholder')}
              disabled={disabled}
              onChange={handleChunkMethodSelectChange}
            >
              {parserList.map((x) => (
                <Option value={x.value} key={x.value}>
                  {x.label}
                </Option>
              ))}
            </Select>
          </Form.Item>
        </Col>
      </Row>
      <Form.Item noStyle dependencies={['parser_id']}>
        {({ getFieldValue }) => {
          const parserId = getFieldValue('parser_id');

          return (
            <>
              {parserId === 'knowledge_graph' && (
                <>
                  <EntityTypesItem></EntityTypesItem>
                  <MaxTokenNumber max={8192 * 2}></MaxTokenNumber>
                  <Delimiter></Delimiter>
                </>
              )}
              {showAutoKeywords(parserId) && (
                <>
                  <Row gutter={32}>
                    <Col span={12}>
                      <AutoKeywordsItem></AutoKeywordsItem>
                    </Col>
                    <Col span={12}>
                      <AutoQuestionsItem></AutoQuestionsItem>
                    </Col>
                  </Row>
                </>
              )}
              {parserId === 'naive' && (
                <>
                  <Row gutter={32}>
                    <Col span={12}>
                      <MaxTokenNumber></MaxTokenNumber>
                    </Col>
                    <Col span={12}>
                      <Delimiter></Delimiter>
                    </Col>
                  </Row>
                  <Row gutter={32}>
                    <Col span={12}>
                      <LayoutRecognize></LayoutRecognize>
                    </Col>
                    <Col span={12}>
                      <ExcelToHtml></ExcelToHtml>
                    </Col>
                  </Row>
                </>
              )}

              {showRaptorParseConfiguration(parserId) && (
                <ParseConfiguration></ParseConfiguration>
              )}
            </>
          );
        }}
      </Form.Item>
      <Form.Item>
        <div className={styles.buttonWrapper}>
          <Space>
            <Button size={'middle'} onClick={navigateToDataset}>
              {t('cancel')}
            </Button>
            <Button
              type="primary"
              size={'middle'}
              loading={submitLoading}
              onClick={submitKnowledgeConfiguration}
            >
              {t('save')}
            </Button>
          </Space>
        </div>
      </Form.Item>
    </Form>
  );
};

export default ConfigurationForm;
