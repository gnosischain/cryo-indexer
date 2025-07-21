"""
Cryo Indexer - Simplified entry point
"""
import sys
import os

# Add migrations command if requested
if len(sys.argv) > 1 and sys.argv[1] == "migrations":
    from .db.migrations import run_migrations
    run_migrations()
else:
    from .indexer import CryoIndexer
    
    # Run the indexer
    indexer = CryoIndexer()
    indexer.run()