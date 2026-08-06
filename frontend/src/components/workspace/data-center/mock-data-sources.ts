export type MockDataSourceType =
  | "local_dataset"
  | "uploaded_file"
  | "database"
  | "vector_store";

export type MockDataSourceStatus = "ready" | "syncing" | "error" | "disabled";

export interface MockDataSource {
  id: string;
  name: string;
  type: MockDataSourceType;
  status: MockDataSourceStatus;
  description: string;
  path?: string;
  updatedAt: string;
  ownerScope: "thread" | "workspace" | "global";
  selectableInChat: boolean;
}

export const mockDataSources: MockDataSource[] = [
  {
    id: "local-network-traffic",
    name: "network_traffic_ustc_bittorrent",
    type: "local_dataset",
    status: "ready",
    description:
      "Built-in traffic dataset prepared for packet / flow analysis and RAG indexing.",
    path: "/mnt/datasets/network-traffic/USTC-TFC2016/BitTorrent",
    updatedAt: "2026-04-12 18:20",
    ownerScope: "global",
    selectableInChat: true,
  },
  {
    id: "local-employee-demo",
    name: "internal_data_employees",
    type: "local_dataset",
    status: "ready",
    description:
      "Demo employee dataset for SQL-style exploration and data analysis skill flows.",
    path: "/mnt/datasets/internal/internal_data_employees.csv",
    updatedAt: "2026-04-11 09:30",
    ownerScope: "global",
    selectableInChat: true,
  },
  {
    id: "uploaded-sales-q1",
    name: "sales_q1_2026.xlsx",
    type: "uploaded_file",
    status: "syncing",
    description:
      "Recently uploaded workbook waiting to be registered as a persistent data-center source.",
    path: "/uploads/workspace/sales_q1_2026.xlsx",
    updatedAt: "2026-04-12 19:05",
    ownerScope: "workspace",
    selectableInChat: true,
  },
  {
    id: "db-finance-preview",
    name: "finance_postgres_prod",
    type: "database",
    status: "disabled",
    description:
      "Database connection placeholder for the second implementation phase.",
    updatedAt: "2026-04-10 14:00",
    ownerScope: "workspace",
    selectableInChat: false,
  },
];
