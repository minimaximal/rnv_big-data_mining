import pymysql

db = pymysql.connect(host="localhost", user="rnv_importer", password="rnv_importer", database="rnv_big_data_mining")

# creaete a new table called linegroups with one column called linegroup_id
def create_linegroups_table():
    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS linegroups")
    cursor.execute("CREATE TABLE linegroups (linegroup_id VARCHAR(255) PRIMARY KEY)")
    db.commit()
    cursor.close()

# alter table lines and add a new column called linegroup as foreign key to linegroups.linegroup_id
def add_linegroup_column():
   cursor = db.cursor()
   cursor.execute("""
      CREATE PROCEDURE drop_if_exists()
      BEGIN
         DECLARE _count INT;
         SELECT COUNT(*) INTO _count
         FROM INFORMATION_SCHEMA.COLUMNS
         WHERE table_name = 'table_name' AND column_name = 'linegroup';
         IF _count = 1 THEN
               ALTER TABLE table_name DROP COLUMN linegroup;
         END IF;
      END;
   """)
   cursor.execute("CALL drop_if_exists()")
   cursor.execute("DROP PROCEDURE IF EXISTS drop_if_exists")
   cursor.execute("""
      ALTER TABLE `lines`
      ADD COLUMN linegroup VARCHAR(255),
      ADD FOREIGN KEY (linegroup) REFERENCES linegroups(linegroup_id)
   """)
   db.commit()
   cursor.close()


# create linegroup rows and insert them into the linegroups table and add the forigin key to lines create_linegroups_table
# if there is a $ in api_id then it is the line group if there is  E than take the string before the "-" and if not than use use the string after the "-"

def create_linegroups():
   cursor = db.cursor()

   # select all api_ids from the lines table
   cursor.execute("SELECT api_id FROM `lines`")
   rows = cursor.fetchall()

   # insert the linegroup_id into the linegroups table
   for row in rows:
      api_id = row[0]
      if "$" in api_id:
         linegroup_id = "$"
      elif "E" in api_id:
         linegroup_id = api_id.split("-")[0]
      else:
         linegroup_id = api_id.split("-")[1]

      cursor.execute("""
         INSERT INTO linegroups (linegroup_id)
         VALUES (%s)
         ON DUPLICATE KEY UPDATE linegroup_id = linegroup_id
      """, (linegroup_id,))

      # update the linegroup column in the lines table to the linegroup_id of the current line
      cursor.execute("""
         UPDATE `lines`
         SET linegroup = %s
         WHERE api_id = %s
      """, (linegroup_id, api_id))

   db.commit()


# upper two functions are not needed anymore because the linegroups table is already created 
# and the linegroup column is already added to the lines table in the general database creation script
# create_linegroups_table()
# add_linegroup_column()
create_linegroups()
