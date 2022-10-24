use csv::WriterBuilder;
use hashbrown::HashMap;
use serde::{
    ser::{SerializeMap},
    Serialize, Serializer,
};
use std::{
    fs::File,
    io::{LineWriter, Read, Write},
    path::PathBuf,
    str, usize,
};

const FORMAT_LENGTH: usize = 6;
const LOGGER_FIELD_LENGTH: i16 = 55;
const FIELD_NAME_LENGTH: usize = 34;
const FIELD_UNITS_LENGTH: usize = 10;
const MARKER_MESSAGE_LENGTH: usize = 50;
const TYPE_FIELD: &str = "field";
const TYPE_MARKER: &str = "marker";
const BLOCK_TYPE_FIELD: i8 = 0;
const BLOCK_TYPE_MARKER: i8 = 1;
const FIELD_DISPLAY_STYLE_FLOAT: &str = "Float";

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Parsed {
    file_format: String,
    format_version: i16,
    timestamp: i32,
    info_data_start: i16,
    data_begin_index: i32,
    record_length: i16,
    num_logger_fields: i16,
    fields: Vec<LoggerFieldScalar>,
    bit_field_names: String,
    info_data: String,
    data_blocks: Vec<DataBlock>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DataBlockField {
    block_type: i8,
    counter: i8,
    timestamp: u16,
}

type Records = HashMap<String, f64>;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BlockHeader {
    block_type: i8,
    counter: i8,
    timestamp: u16,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LoggerFieldScalar {
    field_type: i8,
    name: String,
    units: String,
    display_style: String,
    scale: f32,
    transform: f32,
    digits: i8,
}

#[derive(Debug)]
struct DataBlock {
    block_type: i8,
    counter: i8,
    timestamp: u16,
    records: Records,
    message: String, // marker block
}

impl Serialize for DataBlock {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.records.len() + 2))?;

        // serialize normal fields
        map.serialize_entry(&"timestamp", &self.timestamp)?;
        map.serialize_entry(
            &"type",
            match self.block_type {
                BLOCK_TYPE_FIELD => TYPE_FIELD,
                BLOCK_TYPE_MARKER => TYPE_MARKER,
                _ => panic!("Unsupported Block Type"),
            },
        )?;

        // serialize either message (marker) or hashmap (records)
        match self.block_type {
            BLOCK_TYPE_FIELD => {
                // serialize hash map
                for (k, v) in &self.records {
                    map.serialize_entry(&k.to_string(), &v)?;
                }
            }
            BLOCK_TYPE_MARKER => map.serialize_entry(&"message", &self.message)?,
            _ => (),
        }

        map.end()
    }
}

pub enum Formats {
    Csv,
    Json,
}

pub fn parse(paths: Vec<&PathBuf>, format: Formats) {
    for path in paths {
        let parsed = parse_single_file(path);

        match &parsed {
            Ok(_) => {}
            Err(e) => return println!("Error in [{}]: {}", path.display(), e),
        }

        match format {
            Formats::Csv => {
                let filepath = path.with_extension("csv");
                save_csv(&parsed.unwrap(), &filepath);
                println!("Generated: {}", filepath.display());
            }
            Formats::Json => {
                let json = serde_json::to_string(&parsed).expect("Unable to serialize the result");
                let filepath = path.with_extension("json");
                File::create(&filepath)
                    .unwrap()
                    .write_all(json.as_bytes())
                    .expect("Unable to save output file");
                println!("Generated: {}", &filepath.display());
            }
        }
    }
}

fn parse_single_file(path: &PathBuf) -> Result<Parsed, &str> {
    let mut file = File::open(path).expect("Unable to open file");
    let mut buff = Vec::new();
    let mut offset: usize = 0;

    file.read_to_end(&mut buff).expect("Unable to read file");

    let mut result = Parsed {
        file_format: "".to_string(),
        format_version: 0,
        timestamp: 0,
        info_data_start: 0,
        data_begin_index: 0,
        record_length: 0,
        num_logger_fields: 0,
        fields: Vec::new(),
        bit_field_names: "".to_string(),
        info_data: "".to_string(),
        data_blocks: Vec::new(),
    };

    result.file_format = parse_string(&buff, &mut offset, FORMAT_LENGTH);

    if result.file_format != "MLVLG" {
      return Err("Unsupported file format");
  }

  result.format_version = parse_i16(&buff, &mut offset);

  if result.format_version != 1 {
      return Err("Unsupported file format version");
  }

    result.timestamp = parse_i32(&buff, &mut offset);
    result.info_data_start = parse_i16(&buff, &mut offset);
    result.data_begin_index = parse_i32(&buff, &mut offset);
    result.record_length = parse_i16(&buff, &mut offset);
    result.num_logger_fields = parse_i16(&buff, &mut offset);

    let logger_fields_length = offset + (result.num_logger_fields * LOGGER_FIELD_LENGTH) as usize;

    while offset < logger_fields_length {
        result.fields.push(LoggerFieldScalar {
            field_type: parse_i8(&buff, &mut offset),
            name: parse_string(&buff, &mut offset, FIELD_NAME_LENGTH),
            units: parse_string(&buff, &mut offset, FIELD_UNITS_LENGTH),
            display_style: match parse_i8(&buff, &mut offset) {
                0 => "Float".to_string(),
                1 => "Hex".to_string(),
                2 => "bits".to_string(),
                3 => "Date".to_string(),
                4 => "On/Off".to_string(),
                5 => "Yes/No".to_string(),
                6 => "High/Low".to_string(),
                7 => "Active/Inactive".to_string(),
                _ => panic!("Unsupported Field Display Style"),
            },
            scale: parse_f32(&buff, &mut offset),
            transform: parse_f32(&buff, &mut offset),
            digits: parse_i8(&buff, &mut offset),
        });
    }

    result.bit_field_names = parse_string(
        &buff,
        &mut offset,
        result.info_data_start as usize - logger_fields_length,
    );

    jump(&mut offset, result.info_data_start as usize);

    result.info_data = parse_string(
        &buff,
        &mut offset,
        (result.data_begin_index - result.info_data_start as i32) as usize,
    );

    jump(&mut offset, result.data_begin_index as usize);

    while offset < buff.len() {
        // TODO: report progress every X record
        let mut records: Records = HashMap::new();
        let header = BlockHeader {
            block_type: parse_i8(&buff, &mut offset),
            counter: parse_i8(&buff, &mut offset),
            timestamp: parse_u16(&buff, &mut offset),
        };
        match header.block_type {
            BLOCK_TYPE_FIELD => {
                for field in result.fields.iter() {
                    records.insert(
                        field.name.to_string(),
                        match field.field_type {
                            // Logger Field – scalar
                            0 => parse_u8(&buff, &mut offset) as f64,
                            1 => parse_i8(&buff, &mut offset) as f64,
                            2 => parse_u16(&buff, &mut offset) as f64,
                            3 => parse_i16(&buff, &mut offset) as f64,
                            4 => parse_u32(&buff, &mut offset) as f64,
                            5 => parse_i32(&buff, &mut offset) as f64,
                            6 => parse_i64(&buff, &mut offset) as f64,
                            7 => parse_f32(&buff, &mut offset) as f64,
                            // Logger Field - Bit
                            10 => parse_u8(&buff, &mut offset) as f64,
                            11 => parse_u16(&buff, &mut offset) as f64,
                            12 => parse_u32(&buff, &mut offset) as f64,
                            _ => panic!("Unsupported Field Type"),
                        },
                    );
                }

                // don't parse "crc" (not needed for now), just advance offset
                advance(&mut offset, std::mem::size_of::<u8>());

                result.data_blocks.push(DataBlock {
                    block_type: header.block_type,
                    counter: header.counter,
                    timestamp: header.timestamp,
                    records,
                    message: "".to_string(),
                });
            }
            BLOCK_TYPE_MARKER => result.data_blocks.push(DataBlock {
                block_type: header.block_type,
                counter: header.counter,
                timestamp: header.timestamp,
                records,
                message: parse_string(&buff, &mut offset, MARKER_MESSAGE_LENGTH),
            }),
            _ => panic!("Unsupported Block Type"),
        };
    }

    Ok(result)
}

fn save_csv(parsed: &Parsed, path: &PathBuf) {
    let line_writer = LineWriter::new(File::create(path).unwrap());
    let mut writer = WriterBuilder::new()
        .delimiter(b'\t')
        .from_writer(line_writer);

    let mut header: Vec<String> = Vec::new();
    parsed
        .fields
        .iter()
        .for_each(|field| header.push(field.name.to_string()));

    writer.write_record(header).unwrap();

    let mut units: Vec<String> = Vec::new();
    parsed
        .fields
        .iter()
        .for_each(|field| units.push(field.units.to_string()));

    writer.write_record(units).unwrap();

    for block in parsed.data_blocks.iter() {
        let mut row: Vec<String> = Vec::new();

        if block.block_type == BLOCK_TYPE_FIELD {
            for field in parsed.fields.iter() {
                let value = (block.records.get(&field.name).unwrap() + field.transform as f64)
                    * field.scale as f64;

                if field.display_style == FIELD_DISPLAY_STYLE_FLOAT {
                    row.push(format!("{:.1$}", value, field.digits as usize));
                } else {
                    row.push(value.to_string());
                }
            }
            writer.write_record(row).unwrap();
        }
    }

    writer.flush().unwrap();
}

fn advance(offset: &mut usize, length: usize) {
    *offset += length;
}

fn jump(offset: &mut usize, to: usize) {
    *offset = to;
}

fn parse_string(buff: &[u8], offset: &mut usize, length: usize) -> String {
    let val = str::from_utf8(&buff[*offset..(*offset + length)])
        .expect("Unable to parse string")
        .trim_matches(char::from(0))
        .to_string();
    advance(offset, length);
    val
}

fn parse_i8(buff: &[u8], offset: &mut usize) -> i8 {
    let length = std::mem::size_of::<i8>();
    let buff = &buff[*offset..(*offset + length)];
    advance(offset, length);
    i8::from_be_bytes([buff[0]])
}

fn parse_u8(buff: &[u8], offset: &mut usize) -> u8 {
    let length = std::mem::size_of::<u8>();
    let buff = &buff[*offset..(*offset + length)];
    advance(offset, length);
    u8::from_be_bytes([buff[0]])
}

fn parse_i16(buff: &[u8], offset: &mut usize) -> i16 {
    let length = std::mem::size_of::<i16>();
    let buff = &buff[*offset..(*offset + length)];
    advance(offset, length);
    i16::from_be_bytes([buff[0], buff[1]])
}

fn parse_u16(buff: &[u8], offset: &mut usize) -> u16 {
    let length = std::mem::size_of::<u16>();
    let buff = &buff[*offset..(*offset + length)];
    advance(offset, length);
    u16::from_be_bytes([buff[0], buff[1]])
}

fn parse_i32(buff: &[u8], offset: &mut usize) -> i32 {
    let length = std::mem::size_of::<i32>();
    let buff = &buff[*offset..(*offset + length)];
    advance(offset, length);
    i32::from_be_bytes([buff[0], buff[1], buff[2], buff[3]])
}

fn parse_u32(buff: &[u8], offset: &mut usize) -> u32 {
    let length = std::mem::size_of::<u32>();
    let buff = &buff[*offset..(*offset + length)];
    advance(offset, length);
    u32::from_be_bytes([buff[0], buff[1], buff[2], buff[3]])
}

fn parse_f32(buff: &[u8], offset: &mut usize) -> f32 {
    let length = std::mem::size_of::<f32>();
    let buff = &buff[*offset..(*offset + length)];
    advance(offset, length);
    f32::from_be_bytes([buff[0], buff[1], buff[2], buff[3]])
}

fn parse_i64(buff: &[u8], offset: &mut usize) -> i64 {
    let length = std::mem::size_of::<i64>();
    let buff = &buff[*offset..(*offset + length)];
    advance(offset, length);
    i64::from_be_bytes([
        buff[0], buff[1], buff[2], buff[3], buff[4], buff[5], buff[6], buff[7],
    ])
}
