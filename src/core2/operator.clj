(ns core2.operator
  (:require [core2.operator.group-by :as group-by]
            [core2.operator.join :as join]
            [core2.operator.order-by :as order-by]
            [core2.operator.project :as project]
            [core2.operator.rename :as rename]
            [core2.operator.scan :as scan]
            [core2.operator.select :as select]
            [core2.operator.slice :as slice])
  (:import core2.buffer_pool.BufferPool
           core2.metadata.IMetadataManager
           org.apache.arrow.memory.BufferAllocator))

(definterface IOperatorFactory
  (^core2.ICursor scan [^core2.tx.Watermark watermark
                        ^java.util.List #_<String> colNames,
                        metadataPred
                        ^java.util.Map #_#_<String, IVectorPredicate> colPreds])

  (^core2.ICursor select [^core2.ICursor inCursor
                          ^core2.select.IVectorSchemaRootPredicate pred])

  (^core2.ICursor project [^core2.ICursor inCursor
                           ^java.util.List #_<ProjectionSpec> projectionSpecs])

  (^core2.ICursor rename [^core2.ICursor inCursor
                          ^java.util.Map #_#_<String, String> renameMap])

  (^core2.ICursor equiJoin [^core2.ICursor leftCursor
                            ^String leftColName
                            ^core2.ICursor rightCursor
                            ^String rightColName])

  (^core2.ICursor crossJoin [^core2.ICursor leftCursor
                             ^core2.ICursor rightCursor])

  (^core2.ICursor orderBy [^core2.ICursor inCursor
                           ^java.util.List #_<SortSpec> orderSpecs])

  (^core2.ICursor groupBy [^core2.ICursor inCursor
                           ^java.util.List #_<AggregateSpec> aggregateSpecs])

  (^core2.ICursor slice [^core2.ICursor inCursor, ^Long offset, ^Long limit]))

(defn ->operator-factory
  ^core2.operator.IOperatorFactory
  [^BufferAllocator allocator
   ^IMetadataManager metadata-mgr
   ^BufferPool buffer-pool]

  (reify IOperatorFactory
    (scan [_ watermark col-names metadata-pred col-preds]
      (scan/->scan-cursor allocator metadata-mgr buffer-pool
                          watermark col-names metadata-pred col-preds))

    (select [_ in-cursor pred]
      (select/->select-cursor allocator in-cursor pred))

    (project [_ in-cursor projection-specs]
      (project/->project-cursor allocator in-cursor projection-specs))

    (rename [_ in-cursor rename-map]
      (rename/->rename-cursor allocator in-cursor rename-map))

    (equiJoin [_ left-cursor left-column-name right-cursor right-column-name]
      (join/->equi-join-cursor allocator left-cursor left-column-name right-cursor right-column-name))

    (crossJoin [_ left-cursor right-cursor]
      (join/->cross-join-cursor allocator left-cursor right-cursor))

    (groupBy [_ in-cursor aggregate-specs]
      (group-by/->group-by-cursor allocator in-cursor aggregate-specs))

    (orderBy [_ in-cursor order-specs]
      (order-by/->order-by-cursor allocator in-cursor order-specs))

    (slice [_ in-cursor offset limit]
      (slice/->slice-cursor in-cursor offset limit))))
