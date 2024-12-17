const sql = require("mssql");
const fs = require("fs-extra");
const path = require("path");
const PDFDocument = require("pdfkit");
const { Storage } = require("@google-cloud/storage");
const { v4: uuidv4 } = require("uuid");
const winston = require("winston");
const vision = require("@google-cloud/vision");

const moment = require("moment");

const OUTPUT_JSON_FILE = "./outputData.json"; // Path to save JSON data

// Configure logger
const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(
      ({ timestamp, level, message }) =>
        `${timestamp} [${level.toUpperCase()}]: ${message}`
    )
  ),
  transports: [
    new winston.transports.File({ filename: "app.log" }),
    new winston.transports.Console(),
  ],
});

// SQL Server configuration
const sqlConfig = {
  user: "perigordDevDB",
  password: "PD39262!ymn*",
  server: "172.16.100.16",
  database: "coe",
  port: 1433,
  options: {
    encrypt: true,
    trustServerCertificate: true,
  },
};

// Google Cloud configuration
const keyFilePath = "./gcs-key/perigord-dev.json";
const storage = new Storage({ keyFilename: keyFilePath });
const bucketName = "perigord-archive";

// Initialize Vision AI client
const visionClient = new vision.ImageAnnotatorClient({
  keyFilename: keyFilePath,
});

// Paths for directories
const NOT_DONE_DIR = "./notDone";
const IMAGES_DIR = "./images";

//function to format DOB
function normalizeDate(dateStr) {
  if (!dateStr || dateStr.toUpperCase() === "NULL") {
    return null; // Return null for invalid or NULL values
  }

  // Clean and standardize input by replacing multiple delimiters with a single "/"
  const cleanedDateStr = dateStr.replace(/[\s,.-]+/g, "/");

  // Try parsing the date with moment.js
  const parsedDate = moment(
    cleanedDateStr,
    ["DD/MM/YYYY", "DD/MM/YY", "YYYY-MM-DD", "YYYY/MM/DD"],
    true
  );

  if (parsedDate.isValid()) {
    return parsedDate.format("YYYY-MM-DD"); // Format the date as YYYY-MM-DD
  } else {
    console.warn(`Invalid date format: ${dateStr}`);
    return null; // Return null for unparseable dates
  }
}

//Format Phone Number
function normalizePhoneNumber(phoneStr) {
  if (!phoneStr || phoneStr.toUpperCase() === "NULL") {
    return null; // Return null for invalid or NULL values
  }

  // Extract the part before "Home"
  const partBeforeHome = phoneStr.split("Home")[0].trim();

  // Remove unwanted characters like spaces, dots, and dashes
  const cleanedNumber = partBeforeHome.replace(/[\s.-]/g, "");

  // Validate the number: it should be digits only and of reasonable length (e.g., 7-15 digits)
  if (/^\d{7,15}$/.test(cleanedNumber)) {
    return cleanedNumber;
  } else {
    console.warn(`Invalid phone number format: ${phoneStr}`);
    return null; // Return null for invalid phone numbers
  }
}

// Function to use Vision AI to extract text from an image
async function extractTextFromImage(filePath) {
  try {
    const [result] = await visionClient.textDetection(filePath);
    const detections = result.textAnnotations;
    if (detections.length > 0) {
      return detections[0].description.trim(); // The first annotation contains the full text
    }
    logger.warn(`No text detected in ${filePath}`);
    return null;
  } catch (err) {
    logger.error(`Error extracting text from ${filePath}: ${err.message}`);
    throw err;
  }
}

// Function to process images and generate JSON
async function processImages() {
  const jsonData = [];
  try {
    const imageFiles = await fs.readdir(IMAGES_DIR);
    for (const file of imageFiles) {
      const filePath = path.join(IMAGES_DIR, file);
      if (path.extname(file).toLowerCase() === ".jpg") {
        const text = await extractTextFromImage(filePath);
        if (text) {
          jsonData.push({
            file,
            data: parseExtractedText(text), // Add your own text parsing logic here
          });
        } else {
          logger.warn(`No valid data for ${file}`);
        }
      } else {
        logger.warn(`${file} is not a valid image file.`);
      }
    }

    // Save the JSON data to a file
    await fs.writeFile(
      OUTPUT_JSON_FILE,
      JSON.stringify(jsonData, null, 2),
      "utf8"
    );
    logger.info(`JSON data saved to ${OUTPUT_JSON_FILE}`);

    return jsonData;
  } catch (err) {
    logger.error(`Error processing images: ${err.message}`);
    throw err;
  }
}

// Function to parse extracted text into a key-value structure
function parseExtractedText(text) {
  const lines = text.split("\n");
  const parsedData = {};
  for (const line of lines) {
    const [key, ...value] = line.split(":");
    if (key && value.length) {
      parsedData[key.trim()] = value.join(":").trim();
    }
  }
  return parsedData;
}

// Function to validate records
function isValidRecord(record) {
  return record["Provider"] === "AON" && record["Company"] === "CIE";
}

// Function to map JSON to SQL table structure
function mapToTableStructure(validJson) {
  return validJson.map((entry) => {
    const record = entry.data;
    return {
      pensionID:
        record["Staff/Pension No."] ||
        record["Staff / Pension No."] ||
        record["Staff/ Pension No."] ||
        record["Staff /Pension No."] ||
        null,
      fName: record["First Name"] || null,
      lName: record["Last Name"] || null,
      pensionProvider: record["Provider"] || null,
      companyName: record["Company"] || null,
      addr1: record["Address 1"] || null,
      addr2: record["Address 2"] || null,
      addr3: record["Address 3"] || null,
      city: null,
      county: null,
      eircode: record["Eircode"] || null,
      ppsn: record["PPS Number"] || null,
      dob: normalizeDate(record["Date of Birth"]) || null,
      phone: normalizePhoneNumber(record["Phone Number (s) Mobile"]) || null,
      emailId: record["Email Address*"] || null,
      pdfName: entry.file,
      uploadType: "PGU",
      uploadDate: new Date().toISOString(),
      docType: "RWS_CIE2024",
    };
  });
}

// Function to create PDF and upload to Google Cloud Storage
async function createPDFAndUpload(fileName, record) {
  const documentID = uuidv4();
  const pdfName = `${record.pensionProvider}_${record.pensionID}_${documentID}.pdf`;
  record.pdfName = pdfName;

  try {
    const folderName = `${record.companyName}`;
    const gcsFileName = `${folderName}/${pdfName}`;
    const metadata = {
      metadata: {
        doctype: "RWS_CIE2024",
        fName: record.fName,
        accountNumber: `${record.pensionID}_${record.ppsn}`,
        workflowName: "AON_COE_CIE",
        lName: record.lName,
        jobID: "NA",
        documentID: documentID,
        uploadedDateTime: record.uploadDate,
      },
    };

    const doc = new PDFDocument({ size: "A4", margin: 0 });
    const gcsStream = storage
      .bucket(bucketName)
      .file(gcsFileName)
      .createWriteStream({
        metadata: metadata,
      });

    doc.image(path.join(IMAGES_DIR, fileName), {
      fit: [595.28, 841.89],
      align: "center",
      valign: "center",
    });
    doc.pipe(gcsStream);
    doc.end();

    await new Promise((resolve, reject) => {
      gcsStream.on("finish", resolve);
      gcsStream.on("error", reject);
    });

    logger.info(`Uploaded ${fileName} as ${gcsFileName}`);
  } catch (err) {
    logger.error(
      `Error creating/uploading PDF for ${fileName}: ${err.message}`
    );
    throw err;
  }
}

// Function to insert data into the database
async function insertDataIntoDB(records) {
  let pool;

  try {
    pool = await sql.connect(sqlConfig);

    for (const record of records) {
      const query = `
        INSERT INTO pensionerTable (
          pensionID, fName, lName, pensionProvider, companyName,
          addr1, addr2, addr3, city, county, eircode,
          ppsn, dob, phone, emailId, pdfName,
          uploadType, uploadDate, docType
        )
        VALUES (
          @pensionID, @fName, @lName, @pensionProvider, @companyName,
          @addr1, @addr2, @addr3, @city, @county, @eircode,
          @ppsn, @dob, @phone, @emailId, @pdfName,
          @uploadType, @uploadDate, @docType
        )`;
      var eirCodeTrim, emailTrim;
      if (record.eircode !== null) {
        // Remove all non-alphanumeric characters (keep only letters and numbers)
        eirCodeTrim = record.eircode.replace(/[^a-zA-Z0-9]/g, "");
      }
      if (record.emailId !== null) {
        emailTrim = record.emailId.replaceAll(" ", "");
      }

      await pool
        .request()
        .input("pensionID", sql.VarChar(50), record.pensionID)
        .input("fName", sql.VarChar(100), record.fName)
        .input("lName", sql.VarChar(100), record.lName)
        .input("pensionProvider", sql.VarChar(50), record.pensionProvider)
        .input("companyName", sql.VarChar(50), record.companyName)
        .input("addr1", sql.VarChar(100), record.addr1)
        .input("addr2", sql.VarChar(100), record.addr2)
        .input("addr3", sql.VarChar(100), record.addr3)
        .input("city", sql.VarChar(50), record.city)
        .input("county", sql.VarChar(50), record.county)
        .input("eircode", sql.VarChar(10), eirCodeTrim)
        .input("ppsn", sql.VarChar(20), record.ppsn)
        .input("dob", sql.VarChar(20), record.dob)
        .input("phone", sql.VarChar(50), record.phone)
        .input("emailId", sql.VarChar(100), emailTrim)
        .input("pdfName", sql.VarChar(200), record.pdfName)
        .input("uploadType", sql.VarChar(5), record.uploadType)
        .input("uploadDate", sql.VarChar(50), record.uploadDate)
        .input("docType", sql.VarChar(20), record.docType)
        .query(query);
    }

    logger.info("Data inserted successfully.");
  } catch (err) {
    logger.error("Error inserting data into database: " + err.message);
    throw err;
  } finally {
    if (pool) {
      await pool.close();
    }
  }
}

// Function to move invalid files
async function moveFile(fileName, destinationDir) {
  try {
    const sourcePath = path.join(IMAGES_DIR, fileName);
    const destinationPath = path.join(destinationDir, fileName);
    await fs.ensureDir(destinationDir);
    await fs.move(sourcePath, destinationPath, { overwrite: true });
    logger.info(`Moved ${fileName} to ${destinationDir}`);
  } catch (err) {
    logger.error(`Error moving ${fileName}: ${err.message}`);
  }
}

// Main processing pipeline
(async () => {
  try {
    logger.info("Starting process...");

    // Process images and generate JSON
    const data = await processImages();

    // Separate valid and invalid records
    const validJson = [];
    const invalidFiles = [];
    for (const entry of data) {
      if (isValidRecord(entry.data)) {
        validJson.push(entry);
      } else {
        invalidFiles.push(entry.file);
      }
    }

    logger.info(`Valid records: ${validJson.length}`);
    logger.info(`Invalid files: ${invalidFiles.length}`);

    // Move invalid files
    for (const file of invalidFiles) {
      await moveFile(file, NOT_DONE_DIR);
    }

    // Process valid records (generate PDFs, upload to GCS, and insert into DB)
    const mappedData = mapToTableStructure(validJson);
    for (const record of mappedData) {
      try {
        await createPDFAndUpload(record.pdfName, record);
        await insertDataIntoDB([record]);
        logger.info(`Processed ${record.pdfName} successfully.`);
      } catch (err) {
        logger.error(`Error processing ${record.pdfName}: ${err.message}`);
        await moveFile(record.pdfName, NOT_DONE_DIR);
      }
    }

    logger.info("Process complete.");
  } catch (err) {
    logger.error(`Error: ${err.message}`);
  }
})();
