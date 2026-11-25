import {
  LlmModelType,
  ModelVariableType,
  settledModelVariableMap,
} from '@/constants/knowledge';
import {
  Col,
  Flex,
  Form,
  InputNumber,
  Row,
  Select,
  Slider,
  Switch,
} from 'antd';
import camelCase from 'lodash/camelCase';

import { useTranslate } from '@/hooks/common-hooks';
import { useComposeLlmOptionsByModelTypes } from '@/hooks/llm-hooks';
import { useCallback, useMemo } from 'react';
import styles from './index.less';

interface IProps {
  prefix?: string;
  formItemLayout?: any;
  handleParametersChange?(value: ModelVariableType): void;
}

const LlmSettingItems = ({ prefix, formItemLayout = {} }: IProps) => {
  const form = Form.useFormInstance();
  const { t } = useTranslate('chat');
  const parameterOptions = Object.values(ModelVariableType).map((x) => ({
    label: t(camelCase(x)),
    value: x,
  }));

  const handleParametersChange = useCallback(
    (value: ModelVariableType) => {
      const variable = settledModelVariableMap[value];
      let nextVariable: Record<string, any> = variable;
      if (prefix) {
        nextVariable = { [prefix]: variable };
      }
      form.setFieldsValue(nextVariable);
    },
    [form, prefix],
  );

  const memorizedPrefix = useMemo(() => (prefix ? [prefix] : []), [prefix]);

  const modelOptions = useComposeLlmOptionsByModelTypes([
    LlmModelType.Chat,
    LlmModelType.Image2text,
  ]);

  const handleTtsChange = useCallback((checked: boolean) => {
    // if (checked && !data.tts_id) {
    //   message.error(`Please set TTS model firstly.
    //   Setting >> Model Providers >> System model settings`);
    //   form.setFieldValue(['prompt_config', 'tts'], false);
    // }
  }, []);

  return (
    <>
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            label={t('model')}
            name="llm_id"
            tooltip={t('modelTip')}
            {...formItemLayout}
            rules={[{ required: true, message: t('modelMessage') }]}
          >
            <Select options={modelOptions} showSearch />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item
            label={t('freedom')}
            name="parameter"
            tooltip={t('freedomTip')}
            {...formItemLayout}
            initialValue={ModelVariableType.Precise}
          >
            <Select<ModelVariableType>
              options={parameterOptions}
              onChange={handleParametersChange}
            />
          </Form.Item>
        </Col>
      </Row>

      {/* <Divider></Divider> */}
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            label={t('temperature')}
            tooltip={t('temperatureTip')}
            {...formItemLayout}
          >
            <Flex gap={20} align="center">
              <Form.Item
                name={'temperatureEnabled'}
                valuePropName="checked"
                noStyle
              >
                <Switch size="small" />
              </Form.Item>
              <Form.Item noStyle dependencies={['temperatureEnabled']}>
                {({ getFieldValue }) => {
                  const disabled = !getFieldValue('temperatureEnabled');
                  return (
                    <>
                      <Flex flex={1}>
                        <Form.Item
                          name={[...memorizedPrefix, 'temperature']}
                          noStyle
                        >
                          <Slider
                            className={styles.variableSlider}
                            max={1}
                            step={0.01}
                            disabled={disabled}
                          />
                        </Form.Item>
                      </Flex>
                      <Form.Item
                        name={[...memorizedPrefix, 'temperature']}
                        noStyle
                      >
                        <InputNumber
                          className={styles.sliderInputNumber}
                          max={1}
                          min={0}
                          step={0.01}
                          disabled={disabled}
                        />
                      </Form.Item>
                    </>
                  );
                }}
              </Form.Item>
            </Flex>
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item
            label={t('topP')}
            tooltip={t('topPTip')}
            {...formItemLayout}
          >
            <Flex gap={20} align="center">
              <Form.Item name={'topPEnabled'} valuePropName="checked" noStyle>
                <Switch size="small" />
              </Form.Item>
              <Form.Item noStyle dependencies={['topPEnabled']}>
                {({ getFieldValue }) => {
                  const disabled = !getFieldValue('topPEnabled');
                  return (
                    <>
                      <Flex flex={1}>
                        <Form.Item name={[...memorizedPrefix, 'top_p']} noStyle>
                          <Slider
                            className={styles.variableSlider}
                            max={1}
                            step={0.01}
                            disabled={disabled}
                          />
                        </Form.Item>
                      </Flex>
                      <Form.Item name={[...memorizedPrefix, 'top_p']} noStyle>
                        <InputNumber
                          className={styles.sliderInputNumber}
                          max={1}
                          min={0}
                          step={0.01}
                          disabled={disabled}
                        />
                      </Form.Item>
                    </>
                  );
                }}
              </Form.Item>
            </Flex>
          </Form.Item>
        </Col>
      </Row>
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            label={t('presencePenalty')}
            tooltip={t('presencePenaltyTip')}
            {...formItemLayout}
          >
            <Flex gap={20} align="center">
              <Form.Item
                name={'presencePenaltyEnabled'}
                valuePropName="checked"
                noStyle
              >
                <Switch size="small" />
              </Form.Item>
              <Form.Item noStyle dependencies={['presencePenaltyEnabled']}>
                {({ getFieldValue }) => {
                  const disabled = !getFieldValue('presencePenaltyEnabled');
                  return (
                    <>
                      <Flex flex={1}>
                        <Form.Item
                          name={[...memorizedPrefix, 'presence_penalty']}
                          noStyle
                        >
                          <Slider
                            className={styles.variableSlider}
                            max={1}
                            step={0.01}
                            disabled={disabled}
                          />
                        </Form.Item>
                      </Flex>
                      <Form.Item
                        name={[...memorizedPrefix, 'presence_penalty']}
                        noStyle
                      >
                        <InputNumber
                          className={styles.sliderInputNumber}
                          max={1}
                          min={0}
                          step={0.01}
                          disabled={disabled}
                        />
                      </Form.Item>
                    </>
                  );
                }}
              </Form.Item>
            </Flex>
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item
            label={t('frequencyPenalty')}
            tooltip={t('frequencyPenaltyTip')}
            {...formItemLayout}
          >
            <Flex gap={20} align="center">
              <Form.Item
                name={'frequencyPenaltyEnabled'}
                valuePropName="checked"
                noStyle
              >
                <Switch size="small" />
              </Form.Item>
              <Form.Item noStyle dependencies={['frequencyPenaltyEnabled']}>
                {({ getFieldValue }) => {
                  const disabled = !getFieldValue('frequencyPenaltyEnabled');
                  return (
                    <>
                      <Flex flex={1}>
                        <Form.Item
                          name={[...memorizedPrefix, 'frequency_penalty']}
                          noStyle
                        >
                          <Slider
                            className={styles.variableSlider}
                            max={1}
                            step={0.01}
                            disabled={disabled}
                          />
                        </Form.Item>
                      </Flex>
                      <Form.Item
                        name={[...memorizedPrefix, 'frequency_penalty']}
                        noStyle
                      >
                        <InputNumber
                          className={styles.sliderInputNumber}
                          max={1}
                          min={0}
                          step={0.01}
                          disabled={disabled}
                        />
                      </Form.Item>
                    </>
                  );
                }}
              </Form.Item>
            </Flex>
          </Form.Item>
        </Col>
      </Row>
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            label={t('maxTokens')}
            tooltip={t('maxTokensTip')}
            {...formItemLayout}
          >
            <Flex gap={20} align="center">
              <Form.Item
                name={'maxTokensEnabled'}
                valuePropName="checked"
                noStyle
              >
                <Switch size="small" />
              </Form.Item>
              <Form.Item noStyle dependencies={['maxTokensEnabled']}>
                {({ getFieldValue }) => {
                  const disabled = !getFieldValue('maxTokensEnabled');

                  return (
                    <>
                      <Flex flex={1}>
                        <Form.Item
                          name={[...memorizedPrefix, 'max_tokens']}
                          noStyle
                        >
                          <Slider
                            className={styles.variableSlider}
                            max={500000}
                            disabled={disabled}
                          />
                        </Form.Item>
                      </Flex>
                      <Form.Item
                        name={[...memorizedPrefix, 'max_tokens']}
                        noStyle
                      >
                        <InputNumber
                          disabled={disabled}
                          className={styles.sliderInputNumber}
                          max={500000}
                          min={0}
                        />
                      </Form.Item>
                    </>
                  );
                }}
              </Form.Item>
            </Flex>
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item
            label={'自规划'}
            valuePropName="checked"
            name={['prompt_config', 'think']}
            initialValue={false}
            {...formItemLayout}
          >
            <Switch size="small" onChange={handleTtsChange} />
          </Form.Item>
        </Col>
      </Row>
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            label={'自反思'}
            valuePropName="checked"
            name={['prompt_config', 'thinkSelf']}
            initialValue={false}
            {...formItemLayout}
          >
            <Switch size="small" onChange={handleTtsChange} />
          </Form.Item>
        </Col>
      </Row>
    </>
  );
};

export default LlmSettingItems;
