"""
Range Consolidator - Merges small adjacent completed ranges in indexing_state table.
Maintains fixed-size ranges as defined by INDEXING_RANGE_SIZE.
"""
from typing import List, Dict, Tuple
from dataclasses import dataclass
from loguru import logger
import time

from .db.clickhouse_manager import ClickHouseManager
from .core.state_manager import StateManager
from .config import settings


@dataclass
class ConsolidateResult:
    """Result of consolidation operation."""
    dataset: str
    original_count: int
    consolidated_count: int
    ranges_merged: int
    duration: float


class RangeConsolidator:
    """
    Consolidates fragmented ranges in indexing_state table.
    
    IMPORTANT: 
    - Only merges adjacent ranges with status='completed'
    - Maintains fixed-size ranges as defined by INDEXING_RANGE_SIZE
    - Sums up rows_indexed when merging
    """
    
    def __init__(
        self,
        clickhouse: ClickHouseManager,
        state_manager: StateManager,
        mode: str
    ):
        self.clickhouse = clickhouse
        self.state_manager = state_manager
        self.mode = mode
        self.database = clickhouse.database
        self.range_size = settings.indexing_range_size
        
        logger.info(f"RangeConsolidator initialized for mode {mode} with range size {self.range_size}")
    
    def consolidate_all_datasets(self, datasets: List[str]) -> Dict[str, ConsolidateResult]:
        """Consolidate ranges for all specified datasets."""
        results = {}
        
        logger.info(f"Starting consolidation for {len(datasets)} datasets")
        logger.info(f"Fixed range size: {self.range_size} blocks")
        
        for dataset in datasets:
            start_time = time.time()
            
            try:
                result = self._consolidate_dataset(dataset)
                result.duration = time.time() - start_time
                results[dataset] = result
                
                if result.ranges_merged > 0:
                    logger.info(
                        f"✓ {dataset}: Consolidated {result.original_count} ranges "
                        f"into {result.consolidated_count} "
                        f"(merged {result.ranges_merged} ranges)"
                    )
                else:
                    logger.info(f"✓ {dataset}: No consolidation needed - ranges already optimal")
                
            except Exception as e:
                logger.error(f"Error consolidating {dataset}: {e}")
                results[dataset] = ConsolidateResult(
                    dataset=dataset,
                    original_count=0,
                    consolidated_count=0,
                    ranges_merged=0,
                    duration=time.time() - start_time
                )
        
        self._print_summary(results)
        return results
    
    def _consolidate_dataset(self, dataset: str) -> ConsolidateResult:
        """Consolidate ranges for a single dataset maintaining fixed-size ranges."""
        # Get all completed ranges for this dataset
        ranges = self._get_completed_ranges(dataset)
        
        if len(ranges) < 2:
            return ConsolidateResult(
                dataset=dataset,
                original_count=len(ranges),
                consolidated_count=len(ranges),
                ranges_merged=0,
                duration=0.0
            )
        
        original_count = len(ranges)
        
        # Sort by start_block
        ranges.sort(key=lambda r: r['start_block'])
        
        # Group ranges by their aligned range boundaries
        range_groups = {}
        for r in ranges:
            # Calculate which fixed-size range this belongs to
            aligned_start = (r['start_block'] // self.range_size) * self.range_size
            aligned_end = aligned_start + self.range_size
            
            key = (aligned_start, aligned_end)
            if key not in range_groups:
                range_groups[key] = []
            range_groups[key].append(r)
        
        # Process each group that has multiple entries
        ranges_merged = 0
        for (aligned_start, aligned_end), group in range_groups.items():
            if len(group) > 1:
                # Verify all ranges in the group are adjacent and complete the fixed range
                if self._can_consolidate_group(group, aligned_start, aligned_end):
                    self._consolidate_group(dataset, group, aligned_start, aligned_end)
                    ranges_merged += len(group) - 1
                else:
                    logger.debug(
                        f"Cannot consolidate group for {dataset} "
                        f"({aligned_start}-{aligned_end}): gaps exist"
                    )
        
        consolidated_count = original_count - ranges_merged
        
        return ConsolidateResult(
            dataset=dataset,
            original_count=original_count,
            consolidated_count=consolidated_count,
            ranges_merged=ranges_merged,
            duration=0.0
        )
    
    def _can_consolidate_group(self, group: List[Dict], aligned_start: int, aligned_end: int) -> bool:
        """Check if a group of ranges can be consolidated into a single fixed-size range."""
        # Sort by start block
        group.sort(key=lambda r: r['start_block'])
        
        # Check if ranges are contiguous and cover the entire fixed range
        expected_start = aligned_start
        for r in group:
            if r['start_block'] != expected_start:
                return False
            expected_start = r['end_block']
        
        # Check if we cover the entire range
        return group[0]['start_block'] == aligned_start and group[-1]['end_block'] == aligned_end
    
    def _consolidate_group(self, dataset: str, group: List[Dict], aligned_start: int, aligned_end: int):
        """Consolidate a group of adjacent ranges into a single fixed-size range."""
        if len(group) < 2:
            return
        
        try:
            client = self.clickhouse._connect()
            
            # Sum up all rows indexed
            total_rows = sum(r['rows_indexed'] for r in group)
            
            logger.debug(
                f"Consolidating {len(group)} ranges for {dataset}: "
                f"{aligned_start}-{aligned_end} ({total_rows} total rows)"
            )
            
            # Sort to ensure we keep the first entry
            group.sort(key=lambda r: r['start_block'])
            
            # Update the first entry to cover the entire fixed range
            update_query = f"""
            ALTER TABLE {self.database}.indexing_state
            UPDATE 
                start_block = {aligned_start},
                end_block = {aligned_end},
                rows_indexed = {total_rows},
                batch_id = 'consolidated',
                version = now()
            WHERE mode = '{self.mode}'
              AND dataset = '{dataset}'
              AND start_block = {group[0]['start_block']}
              AND end_block = {group[0]['end_block']}
              AND status = 'completed'
            """
            client.command(update_query)
            
            # Delete the other entries in the group
            if len(group) > 1:
                # Batch delete for efficiency
                conditions = []
                for entry in group[1:]:
                    conditions.append(
                        f"(start_block = {entry['start_block']} AND end_block = {entry['end_block']})"
                    )
                
                delete_query = f"""
                ALTER TABLE {self.database}.indexing_state
                DELETE WHERE mode = '{self.mode}'
                  AND dataset = '{dataset}'
                  AND status = 'completed'
                  AND ({' OR '.join(conditions)})
                """
                client.command(delete_query)
            
            logger.debug(f"Successfully consolidated group for {dataset}")
            
        except Exception as e:
            logger.error(f"Error consolidating group: {e}")
            raise
    
    def _get_completed_ranges(self, dataset: str) -> List[Dict]:
        """
        Get all completed ranges for a dataset.
        Only completed ranges are safe to merge.
        """
        try:
            client = self.clickhouse._connect()
            
            query = f"""
            SELECT 
                start_block,
                end_block,
                rows_indexed,
                status
            FROM {self.database}.indexing_state
            WHERE mode = '{self.mode}'
              AND dataset = '{dataset}'
              AND status = 'completed'
            ORDER BY start_block
            """
            
            result = client.query(query)
            
            ranges = []
            for row in result.result_rows:
                if row[3] == 'completed':
                    ranges.append({
                        'start_block': row[0],
                        'end_block': row[1],
                        'rows_indexed': row[2] or 0
                    })
            
            logger.debug(f"Found {len(ranges)} completed ranges for {dataset}")
            return ranges
            
        except Exception as e:
            logger.error(f"Error getting completed ranges: {e}")
            return []
    
    def _print_summary(self, results: Dict[str, ConsolidateResult]):
        """Print consolidation summary."""
        print("\n" + "=" * 80)
        print("RANGE CONSOLIDATION SUMMARY")
        print("=" * 80)
        print(f"Fixed range size: {self.range_size} blocks")
        
        total_original = 0
        total_consolidated = 0
        total_merged = 0
        
        for dataset, result in results.items():
            print(f"\n{dataset}:")
            print(f"  Original ranges: {result.original_count}")
            print(f"  Consolidated ranges: {result.consolidated_count}")
            print(f"  Ranges merged: {result.ranges_merged}")
            print(f"  Duration: {result.duration:.2f}s")
            
            if result.original_count > 0 and result.ranges_merged > 0:
                reduction_pct = (1 - result.consolidated_count / result.original_count) * 100
                print(f"  Reduction: {reduction_pct:.1f}%")
            
            total_original += result.original_count
            total_consolidated += result.consolidated_count
            total_merged += result.ranges_merged
        
        print(f"\nTOTAL:")
        print(f"  Original: {total_original}")
        print(f"  Consolidated: {total_consolidated}")
        print(f"  Merged: {total_merged}")
        
        if total_original > 0 and total_merged > 0:
            total_reduction = (1 - total_consolidated / total_original) * 100
            print(f"  Overall reduction: {total_reduction:.1f}%")
        
        print("=" * 80 + "\n")