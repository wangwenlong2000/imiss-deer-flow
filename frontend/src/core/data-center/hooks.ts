import { useQuery } from "@tanstack/react-query";

import { getDataSourceDetail, listDataSources } from "./api";

export function useDataSources() {
  return useQuery({
    queryKey: ["data-center", "sources"],
    queryFn: listDataSources,
    refetchOnWindowFocus: false,
  });
}

export function useDataSourceDetail(sourceId: string | null | undefined) {
  return useQuery({
    queryKey: ["data-center", "sources", sourceId],
    queryFn: () => getDataSourceDetail(sourceId!),
    enabled: Boolean(sourceId),
    refetchOnWindowFocus: false,
  });
}
