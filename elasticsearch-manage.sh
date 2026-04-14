#!/bin/bash

# Elasticsearch 管理脚本

ES_URL="http://127.0.0.1:9200"

case "$1" in
  start)
    echo "启动 Elasticsearch..."
    echo 'willdw9030' | sudo -S systemctl start elasticsearch.service
    echo "等待启动..."
    sleep 20
    if curl -s $ES_URL > /dev/null; then
      echo "✓ Elasticsearch 启动成功"
      curl -s $ES_URL | python3 -m json.tool 2>/dev/null | head -10
    else
      echo "✗ 启动失败，请查看日志: sudo journalctl -u elasticsearch.service"
      exit 1
    fi
    ;;
  stop)
    echo "停止 Elasticsearch..."
    echo 'willdw9030' | sudo -S systemctl stop elasticsearch.service
    echo "✓ 已停止"
    ;;
  restart)
    echo "重启 Elasticsearch..."
    echo 'willdw9030' | sudo -S systemctl restart elasticsearch.service
    echo "等待重启..."
    sleep 20
    if curl -s $ES_URL > /dev/null; then
      echo "✓ Elasticsearch 重启成功"
    else
      echo "✗ 重启失败"
      exit 1
    fi
    ;;
  status)
    if curl -s $ES_URL > /dev/null; then
      echo "✓ Elasticsearch 运行中"
      echo ""
      echo "集群状态:"
      curl -s $ES_URL/_cluster/health?pretty 2>/dev/null | head -15
      echo ""
      echo "索引列表:"
      curl -s $ES_URL/_cat/indices?v 2>/dev/null
    else
      echo "✗ Elasticsearch 未运行"
      echo 'willdw9030' | sudo -S systemctl status elasticsearch.service 2>&1 | head -10
    fi
    ;;
  logs)
    echo 'willdw9030' | sudo -S tail -50 /var/log/elasticsearch-custom/network-traffic-rag.log
    ;;
  count)
    INDEX_NAME="${2:-network-traffic-rag}"
    echo "索引 $INDEX_NAME 文档数:"
    curl -s "$ES_URL/$INDEX_NAME/_count" | python3 -m json.tool 2>/dev/null
    ;;
  delete-index)
    INDEX_NAME="${2:-network-traffic-rag}"
    echo "警告: 即将删除索引 $INDEX_NAME"
    read -p "确认删除? (y/n): " confirm
    if [ "$confirm" = "y" ]; then
      curl -X DELETE "$ES_URL/$INDEX_NAME"
      echo "索引已删除"
    else
      echo "取消删除"
    fi
    ;;
  test)
    echo "测试 Elasticsearch 连接..."
    curl -s $ES_URL | python3 -m json.tool
    ;;
  *)
    echo "用法: $0 {start|stop|restart|status|logs|count|delete-index|test}"
    echo ""
    echo "命令说明:"
    echo "  start          - 启动 Elasticsearch"
    echo "  stop           - 停止 Elasticsearch"
    echo "  restart        - 重启 Elasticsearch"
    echo "  status         - 查看运行状态和集群信息"
    echo "  logs           - 查看最新日志"
    echo "  count [索引名]  - 查看索引文档数 (默认: network-traffic-rag)"
    echo "  delete-index [索引名] - 删除索引 (默认: network-traffic-rag)"
    echo "  test           - 测试连接"
    exit 1
    ;;
esac
