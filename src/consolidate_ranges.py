"""
Range Consolidator - Merges small adjacent completed ranges in indexing_state table.
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
    
    IMPORTANT: Only merges adjacent ranges with status='completed'.
    Ranges with other statuses (processing, failed, pending) are preserved as-is.
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
        
        logger.info(f"RangeConsolidator initialized for mode {mode}")
    
    def consolidate_all_datasets(self, datasets: List[str]) -> Dict[str, ConsolidateResult]:
        """Consolidate ranges for all specified datasets."""
        results = {}
        
        logger.info(f"Starting consolidation for {len(datasets)} datasets")
        
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
                    logger.info(f"✓ {dataset}: No adjacent ranges to merge")
                
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
        """Consolidate ranges for a single dataset."""
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
        
        # Find adjacent ranges to merge
        consolidated = []
        current = ranges[0]
        ranges_merged = 0
        
        for next_range in ranges[1:]:
            # Check if ranges are adjacent (no gap)
            if current['end_block'] == next_range['start_block']:
                # Merge ranges
                current = {
                    'start_block': current['start_block'],
                    'end_block': next_range['end_block'],
                    'rows_indexed': current['rows_indexed'] + next_range['rows_indexed']
                }
                ranges_merged += 1
            else:
                # Not adjacent, save current and start new
                consolidated.append(current)
                current = next_range
        
        # Don't forget the last range
        consolidated.append(current)
        
        # Only update if we actually consolidated something
        if len(consolidated) < original_count:
            logger.info(f"Replacing {original_count} ranges with {len(consolidated)} consolidated ranges for {dataset}")
            self._replace_ranges(dataset, ranges, consolidated)
        
        return ConsolidateResult(
            dataset=dataset,
            original_count=original_count,
            consolidated_count=len(consolidated),
            ranges_merged=ranges_merged,
            duration=0.0
        )
    
    def _get_completed_ranges(self, dataset: str) -> List[Dict]:
        """
        Get all completed ranges for a dataset.
        Only completed ranges are safe to merge - other statuses indicate
        ranges that still need processing or have issues.
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
                # Double-check status for safety
                if row[3] == 'completed':
                    ranges.append({
                        'start_block': row[0],
                        'end_block': row[1],
                        'rows_indexed': row[2] or 0
                    })
                else:
                    logger.warning(f"Unexpected non-completed range found: {row}")
            
            logger.debug(f"Found {len(ranges)} completed ranges for {dataset}")
            return ranges
            
        except Exception as e:
            logger.error(f"Error getting completed ranges: {e}")
            return []
    
    def _replace_ranges(self, dataset: str, old_ranges: List[Dict], new_ranges: List[Dict]):
        """Replace old ranges with consolidated ones."""
        try:
            client = self.clickhouse._connect()
            
            # Delete all old ranges
            logger.debug(f"Deleting {len(old_ranges)} old ranges for {dataset}")
            
            for old_range in old_ranges:
                delete_query = f"""
                ALTER TABLE {self.database}.indexing_state
                DELETE WHERE mode = '{self.mode}'
                  AND dataset = '{dataset}'
                  AND start_block = {old_range['start_block']}
                  AND end_block = {old_range['end_block']}
                  AND status = 'completed'
                """
                client.command(delete_query)
            
            # Wait for deletions to propagate
            time.sleep(1)
            
            # Insert consolidated ranges
            logger.debug(f"Inserting {len(new_ranges)} consolidated ranges for {dataset}")
            
            for new_range in new_ranges:
                insert_query = f"""
                INSERT INTO {self.database}.indexing_state
                (mode, dataset, start_block, end_block, status, completed_at, rows_indexed, batch_id)
                VALUES
                ('{self.mode}', '{dataset}', {new_range['start_block']}, 
                 {new_range['end_block']}, 'completed', now(), {new_range['rows_indexed']}, 'consolidated')
                """
                client.command(insert_query)
            
            logger.info(f"Successfully replaced {len(old_ranges)} ranges with {len(new_ranges)} for {dataset}")
            
        except Exception as e:
            logger.error(f"Error replacing ranges: {e}")
            raise
    
    def _print_summary(self, results: Dict[str, ConsolidateResult]):
        """Print consolidation summary."""
        print("\n" + "=" * 80)
        print("RANGE CONSOLIDATION SUMMARY")
        print("=" * 80)
        
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