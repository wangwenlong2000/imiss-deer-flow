import { ReactComponent as MoreModelIcon } from '@/assets/svg/more-model.svg';
import { LlmIcon } from '@/components/svg-icon';
import { useSetModalState, useTranslate } from '@/hooks/common-hooks';
import { LlmItem, useSelectLlmList } from '@/hooks/llm-hooks';
import userService from '@/services/user-service';
import { CloseCircleOutlined, SettingOutlined } from '@ant-design/icons';
import {
  Button,
  Card,
  Col,
  CollapseProps,
  Divider,
  Flex,
  List,
  Row,
  Space,
  Spin,
  Switch,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import { useCallback, useEffect, useMemo, useState } from 'react';
import SettingTitle from '../components/setting-title';
import { isLocalLlmFactory } from '../utils';
import TencentCloudModal from './Tencent-modal';
import ApiKeyModal from './api-key-modal';
import AzureOpenAIModal from './azure-openai-modal';
import BedrockModal from './bedrock-modal';
import FishAudioModal from './fish-audio-modal';
import GoogleModal from './google-modal';
import {
  useHandleDeleteFactory,
  useHandleDeleteLlm,
  useSubmitApiKey,
  useSubmitAzure,
  useSubmitBedrock,
  useSubmitFishAudio,
  useSubmitGoogle,
  useSubmitHunyuan,
  useSubmitOllama,
  useSubmitSpark,
  useSubmitSystemModelSetting,
  useSubmitTencentCloud,
  useSubmitVolcEngine,
  useSubmityiyan,
} from './hooks';
import HunyuanModal from './hunyuan-modal';
import styles from './index.less';
import OllamaModal from './ollama-modal';
import SparkModal from './spark-modal';
import SystemModelSettingModal from './system-model-setting-modal';
import VolcEngineModal from './volcengine-modal';
import YiyanModal from './yiyan-modal';

const { Text } = Typography;
interface IModelCardProps {
  item: LlmItem;
  clickApiKey: (llmFactory: string) => void;
}

const ModelCard = ({ item, clickApiKey }: IModelCardProps) => {
  const { visible, switchVisible } = useSetModalState();
  const { t } = useTranslate('setting');
  const { handleDeleteLlm } = useHandleDeleteLlm(item.name);
  const { handleDeleteFactory } = useHandleDeleteFactory(item.name);
  const [selectedIndex, setSelectedIndex] = useState<number>(-1);

  useEffect(() => {
    const enabledModelIndex = item.llm.findIndex(
      (model) => model.status === '1',
    );
    setSelectedIndex(enabledModelIndex);
  }, [item]);

  const handleApiKeyClick = () => {
    clickApiKey(item.name);
  };

  const handleShowMoreClick = () => {
    switchVisible();
  };

  const onSwitchButtonChange = async (
    values: boolean,
    index: number,
    model: string,
  ) => {
    if (values) {
      const res = await userService.enableModel({
        llm_factory: item.name,
        model: model,
      });
      if (res.data.code === 0) {
        if (values) {
          // 如果要开启当前项，就将选中索引设为当前index
          setSelectedIndex(index);
          // 这里可以添加选中该选项的业务逻辑
          console.log(`选中了: ${name}`);
        } else {
          // 如果要关闭当前项，就清空选中索引
          setSelectedIndex(-1);
          // 这里可以添加取消选中的业务逻辑
          console.log(`取消选中: ${name}`);
        }
      }
    }
  };

  return (
    <List.Item>
      <Card className={styles.addedCard}>
        <Row align={'middle'}>
          <Col span={12}>
            <Flex gap={'middle'} align="center">
              <LlmIcon name={item.name} />
              <Flex vertical gap={'small'}>
                <b>{item.name}</b>
                <Text>{item.tags}</Text>
              </Flex>
            </Flex>
          </Col>
          <Col span={12} className={styles.factoryOperationWrapper}>
            <Space size={'middle'}>
              <Button
                type={'primary'}
                style={{ display: 'flex', alignItems: 'center' }}
                onClick={handleApiKeyClick}
              >
                {isLocalLlmFactory(item.name) ||
                item.name === 'VolcEngine' ||
                item.name === 'Tencent Hunyuan' ||
                item.name === 'XunFei Spark' ||
                item.name === 'BaiduYiyan' ||
                item.name === 'Fish Audio' ||
                item.name === 'Tencent Cloud' ||
                item.name === 'Google Cloud' ||
                item.name === 'Azure OpenAI'
                  ? t('addTheModel')
                  : 'API-Key'}
                <SettingOutlined />
              </Button>
              <Button
                type={'primary'}
                style={{ display: 'flex', alignItems: 'center' }}
                onClick={handleShowMoreClick}
              >
                <Flex gap={'small'}>
                  {t('showMoreModels')}
                  <MoreModelIcon style={{ color: '#FFF' }} />
                </Flex>
              </Button>
              <Button
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  width: '0.3rem',
                  justifyContent: 'center',
                }}
                type={'text'}
                onClick={handleDeleteFactory}
              >
                <CloseCircleOutlined style={{ color: '#D92D20' }} />
              </Button>
            </Space>
          </Col>
        </Row>
        {visible && (
          <List
            size="small"
            dataSource={item.llm}
            className={styles.llmList}
            renderItem={(item, index) => (
              <List.Item>
                <Space>
                  {item.name} <Tag color="#b8b8b8">{item.type}</Tag>
                  <Switch
                    checked={selectedIndex === index}
                    onChange={(checked) =>
                      onSwitchButtonChange(checked, index, item.name)
                    }
                  />
                  <Tooltip title={t('delete', { keyPrefix: 'common' })}>
                    <Button
                      style={{ display: 'flex', alignItems: 'center' }}
                      type={'text'}
                      onClick={handleDeleteLlm(item.name)}
                    >
                      <CloseCircleOutlined style={{ color: '#D92D20' }} />
                    </Button>
                  </Tooltip>
                </Space>
              </List.Item>
            )}
          />
        )}
      </Card>
    </List.Item>
  );
};

const UserSettingModel = () => {
  const { factoryList, myLlmList: llmList, loading } = useSelectLlmList();
  const {
    saveApiKeyLoading,
    initialApiKey,
    llmFactory,
    onApiKeySavingOk,
    apiKeyVisible,
    hideApiKeyModal,
    showApiKeyModal,
  } = useSubmitApiKey();
  const {
    saveSystemModelSettingLoading,
    onSystemSettingSavingOk,
    systemSettingVisible,
    hideSystemSettingModal,
    showSystemSettingModal,
  } = useSubmitSystemModelSetting();
  const { t } = useTranslate('setting');
  const {
    llmAddingVisible,
    hideLlmAddingModal,
    showLlmAddingModal,
    onLlmAddingOk,
    llmAddingLoading,
    selectedLlmFactory,
  } = useSubmitOllama();

  const {
    volcAddingVisible,
    hideVolcAddingModal,
    showVolcAddingModal,
    onVolcAddingOk,
    volcAddingLoading,
  } = useSubmitVolcEngine();

  const {
    HunyuanAddingVisible,
    hideHunyuanAddingModal,
    showHunyuanAddingModal,
    onHunyuanAddingOk,
    HunyuanAddingLoading,
  } = useSubmitHunyuan();

  const {
    GoogleAddingVisible,
    hideGoogleAddingModal,
    showGoogleAddingModal,
    onGoogleAddingOk,
    GoogleAddingLoading,
  } = useSubmitGoogle();

  const {
    TencentCloudAddingVisible,
    hideTencentCloudAddingModal,
    showTencentCloudAddingModal,
    onTencentCloudAddingOk,
    TencentCloudAddingLoading,
  } = useSubmitTencentCloud();

  const {
    SparkAddingVisible,
    hideSparkAddingModal,
    showSparkAddingModal,
    onSparkAddingOk,
    SparkAddingLoading,
  } = useSubmitSpark();

  const {
    yiyanAddingVisible,
    hideyiyanAddingModal,
    showyiyanAddingModal,
    onyiyanAddingOk,
    yiyanAddingLoading,
  } = useSubmityiyan();

  const {
    FishAudioAddingVisible,
    hideFishAudioAddingModal,
    showFishAudioAddingModal,
    onFishAudioAddingOk,
    FishAudioAddingLoading,
  } = useSubmitFishAudio();

  const {
    bedrockAddingLoading,
    onBedrockAddingOk,
    bedrockAddingVisible,
    hideBedrockAddingModal,
    showBedrockAddingModal,
  } = useSubmitBedrock();

  const {
    AzureAddingVisible,
    hideAzureAddingModal,
    showAzureAddingModal,
    onAzureAddingOk,
    AzureAddingLoading,
  } = useSubmitAzure();

  const ModalMap = useMemo(
    () => ({
      Bedrock: showBedrockAddingModal,
      VolcEngine: showVolcAddingModal,
      'Tencent Hunyuan': showHunyuanAddingModal,
      'XunFei Spark': showSparkAddingModal,
      BaiduYiyan: showyiyanAddingModal,
      'Fish Audio': showFishAudioAddingModal,
      'Tencent Cloud': showTencentCloudAddingModal,
      'Google Cloud': showGoogleAddingModal,
      'Azure-OpenAI': showAzureAddingModal,
    }),
    [
      showBedrockAddingModal,
      showVolcAddingModal,
      showHunyuanAddingModal,
      showTencentCloudAddingModal,
      showSparkAddingModal,
      showyiyanAddingModal,
      showFishAudioAddingModal,
      showGoogleAddingModal,
      showAzureAddingModal,
    ],
  );

  const handleAddModel = useCallback(
    (llmFactory: string) => {
      if (isLocalLlmFactory(llmFactory)) {
        showLlmAddingModal(llmFactory);
      } else if (llmFactory in ModalMap) {
        ModalMap[llmFactory as keyof typeof ModalMap]();
      } else {
        showApiKeyModal({ llm_factory: llmFactory });
      }
    },
    [showApiKeyModal, showLlmAddingModal, ModalMap],
  );

  const items: CollapseProps['items'] = [
    {
      key: '1',
      label: t('addedModels'),
      children: (
        <List
          grid={{ gutter: 16, column: 1 }}
          dataSource={llmList}
          renderItem={(item) => (
            <ModelCard item={item} clickApiKey={handleAddModel}></ModelCard>
          )}
        />
      ),
    },
    // {
    //   key: '2',
    //   label: t('modelsToBeAdded'),
    //   children: (
    //     <List
    //       grid={{
    //         column: 6,
    //         gutter: 8,
    //         // xs: 1,
    //         // sm: 1,
    //         // md: 2,
    //         // lg: 3,
    //         // xl: 4,
    //         // xxl: 8,
    //       }}
    //       dataSource={factoryList}
    //       renderItem={(item) => (
    //         <List.Item>
    //           <Card className={styles.toBeAddedCard}>
    //             <Flex vertical gap={'middle'}>
    //               <LlmIcon name={item.name} model={true} />
    //               <Flex vertical gap={'middle'}>
    //                 <b>
    //                   <Text ellipsis={{ tooltip: item.name }}>{item.name}</Text>
    //                 </b>
    //                 <Text className={styles.modelTags}>{item.tags}</Text>
    //               </Flex>
    //             </Flex>
    //             <Divider className={styles.modelDivider}></Divider>
    //             <Button
    //               type="link"
    //               onClick={() => handleAddModel(item.name)}
    //               className={styles.addButton}
    //             >
    //               {t('addTheModel')}
    //             </Button>
    //           </Card>
    //         </List.Item>
    //       )}
    //     />
    //   ),
    // },
  ];

  return (
    <section id="xx" className={styles.modelWrapper}>
      <Spin spinning={loading}>
        <section className={styles.modelContainer}>
          <SettingTitle
            title={t('model')}
            description={t('modelDescription')}
            showRightButton
            clickButton={showSystemSettingModal}
          ></SettingTitle>
          <Divider></Divider>
          <List
            grid={{ gutter: 16, column: 1 }}
            dataSource={llmList}
            renderItem={(item, index) => (
              <ModelCard item={item} clickApiKey={handleAddModel}></ModelCard>
            )}
          />
        </section>
      </Spin>
      <ApiKeyModal
        visible={apiKeyVisible}
        hideModal={hideApiKeyModal}
        loading={saveApiKeyLoading}
        initialValue={initialApiKey}
        onOk={onApiKeySavingOk}
        llmFactory={llmFactory}
      ></ApiKeyModal>
      {systemSettingVisible && (
        <SystemModelSettingModal
          visible={systemSettingVisible}
          onOk={onSystemSettingSavingOk}
          hideModal={hideSystemSettingModal}
          loading={saveSystemModelSettingLoading}
        ></SystemModelSettingModal>
      )}
      <OllamaModal
        visible={llmAddingVisible}
        hideModal={hideLlmAddingModal}
        onOk={onLlmAddingOk}
        loading={llmAddingLoading}
        llmFactory={selectedLlmFactory}
      ></OllamaModal>
      <VolcEngineModal
        visible={volcAddingVisible}
        hideModal={hideVolcAddingModal}
        onOk={onVolcAddingOk}
        loading={volcAddingLoading}
        llmFactory={'VolcEngine'}
      ></VolcEngineModal>
      <HunyuanModal
        visible={HunyuanAddingVisible}
        hideModal={hideHunyuanAddingModal}
        onOk={onHunyuanAddingOk}
        loading={HunyuanAddingLoading}
        llmFactory={'Tencent Hunyuan'}
      ></HunyuanModal>
      <GoogleModal
        visible={GoogleAddingVisible}
        hideModal={hideGoogleAddingModal}
        onOk={onGoogleAddingOk}
        loading={GoogleAddingLoading}
        llmFactory={'Google Cloud'}
      ></GoogleModal>
      <TencentCloudModal
        visible={TencentCloudAddingVisible}
        hideModal={hideTencentCloudAddingModal}
        onOk={onTencentCloudAddingOk}
        loading={TencentCloudAddingLoading}
        llmFactory={'Tencent Cloud'}
      ></TencentCloudModal>
      <SparkModal
        visible={SparkAddingVisible}
        hideModal={hideSparkAddingModal}
        onOk={onSparkAddingOk}
        loading={SparkAddingLoading}
        llmFactory={'XunFei Spark'}
      ></SparkModal>
      <YiyanModal
        visible={yiyanAddingVisible}
        hideModal={hideyiyanAddingModal}
        onOk={onyiyanAddingOk}
        loading={yiyanAddingLoading}
        llmFactory={'BaiduYiyan'}
      ></YiyanModal>
      <FishAudioModal
        visible={FishAudioAddingVisible}
        hideModal={hideFishAudioAddingModal}
        onOk={onFishAudioAddingOk}
        loading={FishAudioAddingLoading}
        llmFactory={'Fish Audio'}
      ></FishAudioModal>
      <BedrockModal
        visible={bedrockAddingVisible}
        hideModal={hideBedrockAddingModal}
        onOk={onBedrockAddingOk}
        loading={bedrockAddingLoading}
        llmFactory={'Bedrock'}
      ></BedrockModal>
      <AzureOpenAIModal
        visible={AzureAddingVisible}
        hideModal={hideAzureAddingModal}
        onOk={onAzureAddingOk}
        loading={AzureAddingLoading}
        llmFactory={'Azure-OpenAI'}
      ></AzureOpenAIModal>
    </section>
  );
};

export default UserSettingModel;
