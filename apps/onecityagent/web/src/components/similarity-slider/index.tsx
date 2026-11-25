import { useTranslate } from '@/hooks/common-hooks';
import { Col, Form, Row, Slider } from 'antd';

type FieldType = {
  similarity_threshold?: number;
  // vector_similarity_weight?: number;
};

interface IProps {
  isTooltipShown?: boolean;
  vectorSimilarityWeightName?: string;
}

const SimilaritySlider = ({
  isTooltipShown = false,
  vectorSimilarityWeightName = 'vector_similarity_weight',
}: IProps) => {
  const { t } = useTranslate('knowledgeDetails');

  return (
    <>
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item<FieldType>
            label={t('similarityThreshold')}
            name={'similarity_threshold'}
            tooltip={isTooltipShown && t('similarityThresholdTip')}
            initialValue={0.2}
          >
            <Slider max={1} step={0.01} />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item
            label={t('vectorSimilarityWeight')}
            name={vectorSimilarityWeightName}
            initialValue={1 - 0.3}
            tooltip={isTooltipShown && t('vectorSimilarityWeightTip')}
          >
            <Slider max={1} step={0.01} />
          </Form.Item>
        </Col>
      </Row>
    </>
  );
};

export default SimilaritySlider;
