#!/usr/bin/env python3
"""
Query chunks_database.mlite - Interactive demo showcasing all query features
"""
from ironbase import IronBase

def main():
    print("=" * 60)
    print("IronBase - Chunks Database Query Demo")
    print("=" * 60)

    # Open database
    db = IronBase("chunks_database.mlite")
    chunks = db.collection("chunks")

    # Stats
    print(f"\n📊 Database Stats:")
    print(f"   Total chunks: {chunks.count_documents()}")

    # Get unique files_ids using distinct()
    files_ids = chunks.distinct("files_id")
    print(f"   Unique files: {len(files_ids)}")

    # Show files
    print(f"\n📁 Files in database:")
    for file_id in files_ids[:5]:  # Show first 5
        chunk_count = chunks.count_documents({"files_id": file_id})
        print(f"   - {file_id}: {chunk_count} chunks")

    # Query examples
    print(f"\n🔍 Query Examples:")

    # 1. Find first chunk of a file
    print("\n   1. First chunk (n=0) of first file:")
    first_chunk = chunks.find_one({"files_id": files_ids[0], "n": 0})
    if first_chunk:
        print(f"      File ID: {first_chunk.get('files_id')}")
        print(f"      Chunk #: {first_chunk.get('n')}")
        print(f"      Data length: {len(first_chunk.get('data', ''))} bytes")

    # 2. Count chunks per file
    print("\n   2. Chunks per file:")
    for file_id in files_ids[:3]:
        count = chunks.count_documents({"files_id": file_id})
        print(f"      {file_id}: {count} chunks")

    # 3. Find all chunks for a specific file (sorted by n)
    if files_ids:
        print(f"\n   3. All chunks for file {files_ids[0]} (sorted by chunk number):")
        file_chunks = chunks.find(
            {"files_id": files_ids[0]},
            sort=[("n", 1)],
            limit=10
        )
        for chunk in file_chunks:
            print(f"      Chunk {chunk['n']}: {len(chunk.get('data', ''))} bytes")

    # 4. Aggregate - count by files_id
    print(f"\n   4. Aggregation - chunks grouped by file:")
    pipeline = [
        {"$group": {"_id": "$files_id", "chunk_count": {"$sum": 1}}},
        {"$sort": {"chunk_count": -1}},
        {"$limit": 5}
    ]
    results = chunks.aggregate(pipeline)
    for result in results:
        print(f"      {result['_id']}: {result['chunk_count']} chunks")

    # 5. Projection - select specific fields only
    print(f"\n   5. Projection - first 3 chunks with only _id and n fields:")
    projected = chunks.find({}, projection={"_id": 1, "n": 1}, limit=3)
    for chunk in projected:
        print(f"      {chunk}")

    # Close
    db.close()

    print("\n" + "=" * 60)
    print("✓ Query demo complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()
