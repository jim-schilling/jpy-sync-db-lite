#!/usr/bin/env python3
"""
Demo of new API improvements: execute_many, script, and transaction context manager.

This example demonstrates the enhanced developer experience with the new API methods.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""

import os
import tempfile

from jpy_sync_db_lite import DbEngine


def main():
    """Demonstrate the new API improvements."""
    # Create a temporary database
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    database_url = f"sqlite:///{temp_db.name}"
    
    try:
        db = DbEngine(database_url)
        
        print("=== API Improvements Demo ===\n")
        
        # 1. Using script() method for setup
        print("1. Using script() method for database setup:")
        setup_script = """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                department TEXT,
                active BOOLEAN DEFAULT 1
            );
            
            CREATE INDEX idx_users_department ON users(department);
            CREATE INDEX idx_users_active ON users(active);
        """
        
        results = db.script(setup_script)
        print(f"   Setup script executed {len(results)} statements successfully")
        
        # 2. Using execute_many() for bulk operations
        print("\n2. Using execute_many() for bulk user insertion:")
        users = [
            {"name": "Alice Johnson", "email": "alice@company.com", "department": "Engineering"},
            {"name": "Bob Smith", "email": "bob@company.com", "department": "Marketing"},
            {"name": "Charlie Brown", "email": "charlie@company.com", "department": "Engineering"},
            {"name": "Diana Prince", "email": "diana@company.com", "department": "Sales"},
            {"name": "Eve Davis", "email": "eve@company.com", "department": "HR"},
        ]
        
        results = db.execute_many(
            "INSERT INTO users (name, email, department) VALUES (:name, :email, :department)",
            users
        )
        print(f"   Inserted {len(results)} users in a single transaction")
        
        # Verify insertions
        user_count = db.fetch("SELECT COUNT(*) as count FROM users")
        print(f"   Total users in database: {user_count.data[0]['count']}")
        
        # 3. Using execute_many() for bulk queries
        print("\n3. Using execute_many() for bulk queries by department:")
        departments = [
            {"dept": "Engineering"},
            {"dept": "Marketing"},
            {"dept": "Sales"},
        ]
        
        dept_results = db.execute_many(
            "SELECT name, email FROM users WHERE department = :dept ORDER BY name",
            departments
        )
        
        for i, (dept, result) in enumerate(zip(departments, dept_results)):
            dept_name = dept["dept"]
            user_list = result.data if result.result else []
            print(f"   {dept_name}: {len(user_list)} users")
            for user in user_list:
                print(f"     - {user['name']} ({user['email']})")
        
        # 4. Using transaction() context manager
        print("\n4. Using transaction() context manager for complex operations:")
        try:
            with db.transaction() as tx:
                # Promote Alice to manager
                tx.execute(
                    "UPDATE users SET department = :new_dept WHERE email = :email",
                    {"new_dept": "Management", "email": "alice@company.com"}
                )
                
                # Add a new team member to replace Alice in Engineering
                tx.execute(
                    "INSERT INTO users (name, email, department) VALUES (:name, :email, :department)",
                    {"name": "Frank Miller", "email": "frank@company.com", "department": "Engineering"}
                )
                
                # Log the change (fetch within transaction)
                tx.fetch("SELECT COUNT(*) as eng_count FROM users WHERE department = 'Engineering'")
            
            print("   Transaction completed successfully!")
            
            # Verify the changes
            alice = db.fetch("SELECT department FROM users WHERE email = 'alice@company.com'")
            eng_count = db.fetch("SELECT COUNT(*) as count FROM users WHERE department = 'Engineering'")
            
            print(f"   Alice's new department: {alice.data[0]['department']}")
            print(f"   Engineering team size: {eng_count.data[0]['count']}")
            
        except Exception as e:
            print(f"   Transaction failed: {e}")
        
        # 5. Demonstrate transaction rollback
        print("\n5. Demonstrating transaction rollback on error:")
        try:
            with db.transaction() as tx:
                tx.execute(
                    "INSERT INTO users (name, email, department) VALUES (:name, :email, :department)",
                    {"name": "Grace Hopper", "email": "grace@company.com", "department": "Engineering"}
                )
                
                # This will fail due to duplicate email
                tx.execute(
                    "INSERT INTO users (name, email, department) VALUES (:name, :email, :department)",
                    {"name": "Duplicate", "email": "alice@company.com", "department": "Engineering"}
                )
                
        except Exception as e:
            print(f"   Transaction rolled back due to error: {type(e).__name__}")
            
            # Verify Grace was not inserted (transaction rolled back)
            grace = db.fetch("SELECT COUNT(*) as count FROM users WHERE name = 'Grace Hopper'")
            print(f"   Grace Hopper in database: {grace.data[0]['count']} (should be 0)")
        
        # 6. Performance comparison demonstration
        print("\n6. Performance comparison - individual vs bulk operations:")
        
        # Create a test table
        db.execute("CREATE TABLE performance_test (id INTEGER PRIMARY KEY, value TEXT)")
        
        import time
        
        # Individual operations
        start_time = time.time()
        for i in range(100):
            db.execute("INSERT INTO performance_test (value) VALUES (:value)", params={"value": f"individual_{i}"})
        individual_time = time.time() - start_time
        
        # Bulk operations
        bulk_data = [{"value": f"bulk_{i}"} for i in range(100)]
        start_time = time.time()
        db.execute_many("INSERT INTO performance_test (value) VALUES (:value)", bulk_data)
        bulk_time = time.time() - start_time
        
        print(f"   Individual operations (100 inserts): {individual_time:.3f}s")
        print(f"   Bulk operation (100 inserts): {bulk_time:.3f}s")
        print(f"   Bulk is {individual_time/bulk_time:.1f}x faster")
        
        # Final stats
        print("\n=== Final Statistics ===")
        stats = db.get_stats()
        for key, value in stats.items():
            print(f"   {key}: {value}")
        
        db.shutdown()
        print("\n✅ Demo completed successfully!")
        
    finally:
        # Clean up
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)


if __name__ == "__main__":
    main()
