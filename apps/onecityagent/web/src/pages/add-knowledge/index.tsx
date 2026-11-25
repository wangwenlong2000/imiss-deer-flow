import {
  useFetchKnowledgeBaseConfiguration,
  useKnowledgeBaseId,
} from '@/hooks/knowledge-hooks';
import {
  useNavigateWithFromState,
  useSecondPathName,
  useThirdPathName,
} from '@/hooks/route-hook';
import { Breadcrumb } from 'antd';
import { ItemType } from 'antd/es/breadcrumb/Breadcrumb';
import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { Link, Outlet } from 'umi';
import Siderbar from './components/knowledge-sidebar';
import { KnowledgeDatasetRouteKey, KnowledgeRouteKey } from './constant';

const KnowledgeAdding = () => {
  const { data: knowledgeDetails } = useFetchKnowledgeBaseConfiguration();
  const knowledgeBaseId = useKnowledgeBaseId();
  const { t } = useTranslation();
  const activeKey: KnowledgeRouteKey =
    (useSecondPathName() as KnowledgeRouteKey) || KnowledgeRouteKey.Dataset;

  const datasetActiveKey: KnowledgeDatasetRouteKey =
    useThirdPathName() as KnowledgeDatasetRouteKey;

  const gotoList = useNavigateWithFromState();
  const breadcrumbItems: ItemType[] = useMemo(() => {
    const items: ItemType[] = [
      {
        title: (
          <a onClick={() => gotoList('/knowledge')}>
            {t('header.knowledgeBase')}
          </a>
        ),
      },
      {
        title: datasetActiveKey ? (
          <Link
            to={`/knowledge/${KnowledgeRouteKey.Dataset}?id=${knowledgeBaseId}`}
          >
            {knowledgeDetails.name}
          </Link>
        ) : (
          knowledgeDetails.name
        ),
      },
    ];

    if (datasetActiveKey) {
      items.push({
        title: t(`knowledgeDetails.${datasetActiveKey}`),
      });
    }

    return items;
  }, [
    activeKey,
    datasetActiveKey,
    gotoList,
    knowledgeBaseId,
    t,
    knowledgeDetails,
  ]);

  return (
    <>
      <Breadcrumb
        style={{
          height: '0.22rem',
          lineHeight: '0.22rem',
          fontSize: '0.14rem',
        }}
        items={breadcrumbItems}
        separator=">"
      />
      <Siderbar></Siderbar>
      {/* <div className={styles.content}> */}
      <Outlet></Outlet>
      {/* </div> */}
    </>
  );
};

export default KnowledgeAdding;
