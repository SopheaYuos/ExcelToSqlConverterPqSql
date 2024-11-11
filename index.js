const xlsx = require('xlsx');
const { Client } = require('pg');
const fs = require('fs');

// Database connection setup
const client = new Client({
  user: 'postgres',
  host: 'localhost',
  database: 'infinity_live_backup',
  password: 'admin',
  port: 5432,
});

// Function to retrieve database columns dynamically
async function getDatabaseColumns() {
  try {
    await client.connect();
    const res = await client.query(`
      SELECT column_name 
      FROM information_schema.columns
      WHERE table_name = 'HCIPClaims';
    `);
    return res.rows.map((row) => row.column_name);
  } catch (err) {
    console.error('Error retrieving columns:', err);
    return [];
  }
}

// Function to convert Excel serial date to a JavaScript Date object
function excelDateToJSDate(excelDate) {
  const excelStartDate = new Date(1900, 0, 1); // January 1, 1900
  return new Date(
    excelStartDate.getTime() + (excelDate - 2) * 24 * 60 * 60 * 1000
  );
}

// Function to format date as 'YYYY-MM-DD'
function formatDate(value) {
  if (!isNaN(value)) {
    value = excelDateToJSDate(value);
  }
  const date = new Date(value);
  if (isNaN(date)) return 'NULL';
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `'${year}-${month}-${day}'`;
}

// Function to load Excel and map columns dynamically
async function mapExcelToDatabase() {
  try {
    // Retrieve database columns
    const dbColumns = await getDatabaseColumns();

    // Load Excel file
    const workbook = xlsx.readFile(
      'C:/Users/user/Downloads/claim_history.xlsx'
    );
    const sheet = workbook.Sheets[workbook.SheetNames[1]];
    const data = xlsx.utils.sheet_to_json(sheet);

    // Generate column mapping based on matching column names
    const excelColumns = Object.keys(data[0]);
    const columnMapping = {};
    excelColumns.forEach((excelCol) => {
      // Convert both the Excel column and DB column to lowercase for case-insensitive comparison
      const normalizedExcelCol = excelCol.toLowerCase();
      const matchingDbCol = dbColumns.find(
        (dbCol) => dbCol.toLowerCase() === normalizedExcelCol
      );

      if (matchingDbCol) {
        columnMapping[excelCol] = matchingDbCol; // Map the original Excel column name to the matched DB column name
      }
    });

    let sqlQueries = [];
    let valueBatch = [];
    const dbColumnNames = Object.keys(columnMapping)
      .map((col) => `"${col}"`)
      .join(', ');

    data.forEach((row) => {
      const values = [];

      for (let excelCol in columnMapping) {
        const dbCol = columnMapping[excelCol];
        let value = row[excelCol];

        if (value !== undefined) {
          if (['ClaimDate', 'CheckIn', 'CheckOut'].includes(dbCol)) {
            value = formatDate(value);
          } else {
            value = `'${value}'`;
          }
          values.push(value);
        }
      }

      valueBatch.push(`(${values.join(', ')})`);
    });

    // After accumulating all the values, insert them at once
    if (valueBatch.length > 0) {
      sqlQueries.push(
        `INSERT INTO data."HCIPClaims" (${dbColumnNames}) \nVALUES \n  ${valueBatch.join(
          ', \n  '
        )};`
      );
    }

    // Write the batched SQL insert statements to a file
    fs.writeFileSync('output.sql', sqlQueries.join('\n\n'));

    console.log('SQL output file generated successfully!');
  } catch (err) {
    console.error('Error processing Excel file:', err);
  } finally {
    await client.end();
  }
}

mapExcelToDatabase();
