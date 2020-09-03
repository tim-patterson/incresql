use crate::{Catalog, CatalogError, DATABASES_TABLE_ID, PREFIX_METADATA_TABLE_ID, TABLES_TABLE_ID};
use data::{DataType, Datum, SortOrder};

impl Catalog {
    /// Function used on first boot to initialize system tables
    pub(crate) fn bootstrap(&mut self) -> Result<(), CatalogError> {
        let mut key_buf = vec![];
        let mut value_buf = vec![];
        // Initialization check
        if self
            .prefix_metadata_table
            .system_point_lookup(
                &[Datum::from(PREFIX_METADATA_TABLE_ID as i64)],
                &mut key_buf,
                &mut value_buf,
            )?
            .is_some()
        {
            return Ok(());
        }

        self.create_database_impl("incresql")?;
        self.create_database_impl("default")?;

        self.create_table_impl(
            "incresql",
            "prefix_tables",
            PREFIX_METADATA_TABLE_ID,
            &[
                ("table_id".to_string(), DataType::BigInt),
                ("column_len".to_string(), DataType::Integer),
                ("pk_sort".to_string(), DataType::Json),
            ],
            &[SortOrder::Asc],
            true,
        )?;

        self.create_table_impl(
            "incresql",
            "databases",
            DATABASES_TABLE_ID,
            &[("name".to_string(), DataType::Text)],
            &[SortOrder::Asc],
            true,
        )?;

        self.create_table_impl(
            "incresql",
            "tables",
            TABLES_TABLE_ID,
            &[
                ("database_name".to_string(), DataType::Text),
                ("name".to_string(), DataType::Text),
                ("type".to_string(), DataType::Text),
                ("sql".to_string(), DataType::Text),
                ("table_id".to_string(), DataType::BigInt),
                ("columns".to_string(), DataType::Json),
                ("system".to_string(), DataType::Boolean),
            ],
            &[SortOrder::Asc, SortOrder::Asc],
            true,
        )?;

        Ok(())
    }
}
