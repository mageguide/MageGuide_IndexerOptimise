<?php
/**
 * Copyright © Magento, Inc. All rights reserved.
 * See COPYING.txt for license details.
 */
namespace MageGuide\IndexerOptimise\Preference\Magento\Catalog\Model\Indexer\Product\Eav\Action;

use Magento\Catalog\Model\ResourceModel\Indexer\ActiveTableSwitcher;

use \Magento\Framework\DB\Adapter\AdapterInterface;

/**
 * Class Full reindex action
 */
class Full extends \Magento\Catalog\Model\Indexer\Product\Eav\Action\Full
{

    private $batchSize = 10;
    /**
     * @var \Magento\Framework\EntityManager\MetadataPool
     */
    private $metadataPool;

    /**
     * @var \Magento\Framework\Indexer\BatchProviderInterface
     */
    private $batchProvider;

    /**
     * @var \Magento\Catalog\Model\ResourceModel\Product\Indexer\Eav\BatchSizeCalculator
     */
    private $batchSizeCalculator;

    /**
     * @var ActiveTableSwitcher
     */
    private $activeTableSwitcher;

    /**
     * @param \Magento\Catalog\Model\ResourceModel\Product\Indexer\Eav\DecimalFactory $eavDecimalFactory
     * @param \Magento\Catalog\Model\ResourceModel\Product\Indexer\Eav\SourceFactory $eavSourceFactory
     * @param \Magento\Framework\EntityManager\MetadataPool|null $metadataPool
     * @param \Magento\Framework\Indexer\BatchProviderInterface|null $batchProvider
     * @param \Magento\Catalog\Model\ResourceModel\Product\Indexer\Eav\BatchSizeCalculator $batchSizeCalculator
     * @param ActiveTableSwitcher|null $activeTableSwitcher
     */
    public function __construct(
        \Magento\Catalog\Model\ResourceModel\Product\Indexer\Eav\DecimalFactory $eavDecimalFactory,
        \Magento\Catalog\Model\ResourceModel\Product\Indexer\Eav\SourceFactory $eavSourceFactory,
        \Magento\Framework\EntityManager\MetadataPool $metadataPool = null,
        \Magento\Framework\Indexer\BatchProviderInterface $batchProvider = null,
        \Magento\Catalog\Model\ResourceModel\Product\Indexer\Eav\BatchSizeCalculator $batchSizeCalculator = null,
        ActiveTableSwitcher $activeTableSwitcher = null,
        \Magento\Eav\Model\Config $eavConfig
    ) {
        parent::__construct(
            $eavDecimalFactory,
            $eavSourceFactory,
            $metadataPool,
            $batchProvider,
            $batchSizeCalculator,
            $activeTableSwitcher
        );
        $this->metadataPool = $metadataPool ?: \Magento\Framework\App\ObjectManager::getInstance()->get(
            \Magento\Framework\EntityManager\MetadataPool::class
        );
        $this->batchProvider = $batchProvider ?: \Magento\Framework\App\ObjectManager::getInstance()->get(
            \Magento\Framework\Indexer\BatchProviderInterface::class
        );
        $this->batchSizeCalculator = $batchSizeCalculator ?: \Magento\Framework\App\ObjectManager::getInstance()->get(
            \Magento\Catalog\Model\ResourceModel\Product\Indexer\Eav\BatchSizeCalculator::class
        );
        $this->activeTableSwitcher = $activeTableSwitcher ?: \Magento\Framework\App\ObjectManager::getInstance()->get(
            ActiveTableSwitcher::class
        );
        $this->eavConfig = $eavConfig;
    }

    /**
     * Execute Full reindex
     *
     * @param array|int|null $ids
     * @return void
     * @throws \Magento\Framework\Exception\LocalizedException
     * @SuppressWarnings(PHPMD.UnusedFormalParameter)
     */
    public function execute($ids = null)
    {
        try {
            foreach ($this->getIndexers() as $indexerName => $indexer) {
                $connection = $indexer->getConnection();
                $mainTable = $this->activeTableSwitcher->getAdditionalTableName($indexer->getMainTable());
                $connection->truncateTable($mainTable);
                $entityMetadata = $this->metadataPool->getMetadata(\Magento\Catalog\Api\Data\ProductInterface::class);

                $batches = $this->getBatches(
                    $connection,
                    $entityMetadata->getEntityTable(),
                    $entityMetadata->getIdentifierField(),
                    $this->batchSize ,
                    //$this->batchSizeCalculator->estimateBatchSize($connection, $indexerName)
                    $connection
                );
                $i = 0;
                foreach ($batches as $batch) {
                    $i++ ;
                    //echo $i . "  ";
                    /** @var \Magento\Framework\DB\Select $select */
                    $select = $connection->select();
                    $select->distinct(true);
                    $select->from(['e' => $entityMetadata->getEntityTable()], $entityMetadata->getIdentifierField());
                    $entityIds = array_map('intval', $batch);
                    if (!empty($entityIds)) {
                        $indexer->reindexEntities($this->processRelations($indexer, $entityIds, true));
                        $this->syncData($indexer, $mainTable);
                    }
                }
                $this->activeTableSwitcher->switchTable($indexer->getConnection(), [$indexer->getMainTable()]);
            }
        } catch (\Exception $e) {
            throw new \Magento\Framework\Exception\LocalizedException(__($e->getMessage()), $e);
        }
    }


    public function getBatches(AdapterInterface $adapter, $tableName, $linkField, $batchSize, $connection )
    {
        $statusAttribute = $this->eavConfig->getAttribute(\Magento\Catalog\Model\Product::ENTITY, 'status')->getId();

        $select = $connection->select()->distinct(true)->from(
            ['entity' => $tableName],
            []
        )->joinLeft(
            ['cpie' => $connection->getTableName('catalog_product_entity_int')],
            'cpie.store_id = 0 AND cpie.entity_id = entity.entity_id AND cpie.attribute_id = '.$statusAttribute,
            []
        )->where(
            'cpie.value = 1'
        )->columns(
            [
                'entity_id' => 'entity.entity_id',
                'attribute_set_id' => 'entity.attribute_set_id'
            ]
        )->order(
            'attribute_set_id','ASC'
        )->order(
            'entity_id','ASC'
        );
        $enabledProducts =  $connection->fetchAll($select);
        $prodcutsPerSet = [];
        $i = $batchSize + 1;
        $index = 0;
        $previousSet = 0;
        foreach ($enabledProducts as $key => $product){
            $i++;
            if ($i > $batchSize || $previousSet != $product['attribute_set_id']){
                $previousSet = $product['attribute_set_id'];;
                $index++;
                $i = 1;
            }

            $prodcutsPerSet[$index][]=$product['entity_id'];
        }
        return $prodcutsPerSet;
    }


    /**
     * @inheritdoc
     */
    protected function syncData($indexer, $destinationTable, $ids = null): void
    {
        $connection = $indexer->getConnection();
        $connection->beginTransaction();
        try {
            $sourceTable = $indexer->getIdxTable();
            $sourceColumns = array_keys($connection->describeTable($sourceTable));
            $targetColumns = array_keys($connection->describeTable($destinationTable));
            $select = $connection->select()->from($sourceTable, $sourceColumns);
            $query = $connection->insertFromSelect(
                $select,
                $destinationTable,
                $targetColumns,
                \Magento\Framework\DB\Adapter\AdapterInterface::INSERT_ON_DUPLICATE
            );
            $connection->query($query);
            $connection->commit();
        } catch (\Exception $e) {
            $connection->rollBack();
            throw $e;
        }
    }
}
