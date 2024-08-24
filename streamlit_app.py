import _snowflake
import json
import time
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session, FileOperation
import io
import yaml
from datetime import datetime
import re 

def send_message(prompt: str) -> dict:
    """Calls the REST API and returns the response."""
    request_body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ],
        "semantic_model_file": f"@{st.session_state['semantic_file_name']}",
    }
    resp = _snowflake.send_snow_api_request(
        "POST",
        f"/api/v2/cortex/analyst/message",
        {},
        {},
        request_body,
        {},
        30000,
    )
    if resp["status"] < 400:
        return json.loads(resp["content"])
    else:
        raise Exception(
            f"Failed request with status {resp['status']}: {resp}"
        )

def process_message(prompt: str) -> None:
    """Processes a message and adds the response to the chat."""
    st.session_state.messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.spinner("Generating response..."):
            response = send_message(prompt=prompt)
            content = response["message"]["content"]
            display_content(content=content)
    st.session_state.messages.append({"role": "assistant", "content": content})


def display_content(content: list, message_index: int = None) -> None:
    """Displays a content item for a message."""
    message_index = message_index or len(st.session_state.messages)
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"{message_index}_{suggestion_index}"):
                        st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            with st.expander("SQL Query", expanded=False):
                st.code(item["statement"], language="sql")
            with st.expander("Results", expanded=True):
                with st.spinner("Running SQL..."):
                    session = get_active_session()
                    df = session.sql(item["statement"]).to_pandas()
                    if len(df.index) > 1:
                        data_tab, line_tab, bar_tab, area_chart = st.tabs(
                            ["Data", "Line Chart", "Bar Chart", "Area Chart"]
                        )
                        data_tab.dataframe(df)
                        if len(df.columns) > 1:
                            df = df.set_index(df.columns[0])
                        with line_tab:
                            st.line_chart(df)
                        with bar_tab:
                            st.bar_chart(df)
                        with area_chart:
                            st.area_chart(df)
                    else:
                        st.dataframe(df)
                        
if "messages" not in st.session_state:
    st.session_state.messages = []
    st.session_state.suggestions = []
    st.session_state.active_suggestion = None
    
def show_cortex_analyst_page():
    st.title("Cortex analyst")
    st.markdown(f"Semantic Model: `{st.session_state['semantic_file_name']}`")

    for message_index, message in enumerate(st.session_state.messages):
        with st.chat_message(message["role"]):
            display_content(content=message["content"], message_index=message_index)
    
    if user_input := st.chat_input("What is your question?"):
        process_message(prompt=user_input)
    
    if st.session_state.active_suggestion:
        process_message(prompt=st.session_state.active_suggestion)
        st.session_state.active_suggestion = None



# Upload semantic model file to stage 

def upload_to_stage(db_name, schema_name, yaml_bytes, stage_name):
    session = get_active_session()
    semantic_file_name = db_name+'.'+ schema_name+'.'+stage_name+'/'+st.session_state['semantic_name']+'_semantic_model_'+datetime.now().strftime("%Y%m%d%H%M")+'.yaml'
    FileOperation(session).put_stream(input_stream=yaml_bytes, stage_location='@'+semantic_file_name, auto_compress=False)
    st.session_state['semantic_file_name'] = semantic_file_name
    
# Function to get MIN/MAX of a column 

def min_max_column(database_name, schema_name, table_name, column): 
    session = get_active_session()

    table_definition_df = session.sql(f"SELECT MIN({column})::TEXT AS MIN, MAX({column})::TEXT as MAX FROM {database_name}.{schema_name}.{table_name}").collect()

    return table_definition_df[0]['MIN'], table_definition_df[0]['MAX']
    
# Function to Generate Column description using Cortex LLM (Complete)

def generate_column_description(database_name, schema_name, table_name, columns, column_name, column_details):
    session = get_active_session()

    # Create the prompt
    prompt = f"""
    Please provide a concise business description for the column in two phrases or less. Do not mention the column name, its type, whether it is unique or nullable, where it is stored, or any other technical details. I only need the business description. Do not include any special characters or additional information beyond the business description.
    
    The column name is: {column_name}
    It is stored in the {table_name} table, which is in the {schema_name} schema, in the {database_name} database.
    Other columns in the same table are: {columns}
    
    Use the following information to enhance the description:
    {column_details}
    
    Focus solely on the business purpose and usage of the column in queries. Avoid any mention of technical attributes such as nullability, data type, or storage location.
    Do not include any introductory phrases.
    
    Example:
    For a column named "COGS" in the "DAILY_REVENUE" table, which is in the "REVENUE_TIMESERIES" schema, in the "CORTEX_ANALYST_DEMO" database, with the following details:
    {{
        "NAME": "COGS",
        "TYPE": "FLOAT",
        "KIND": "COLUMN",
        "null?": "Y",
        "DEFAULT": None,
        "primary key": "N",
        "unique key": "N",
        "CHECK": None,
        "EXPRESSION": None,
        "COMMENT": None,
        "policy name": None,
        "privacy domain": None
    }}
    The good description might be:
    "Represents the cost of goods sold"
    The bad description would be: 
    "Here is a concise business description for the COGS column Represents the cost of goods sold"
    """

    # Create the SQL query
    query = f"""SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3-70b', '{prompt.replace("'", "''")}') as response;"""
    
    generated_description = session.sql(query).collect()
    
    return re.sub('[^A-Za-z0-9]+', ' ', generated_description[0]['RESPONSE']).replace('"', "").strip().replace('\n', ' ')

def generate_table_description(database_name, schema_name, table_name, column_details):
    session = get_active_session()

    # Create the prompt
    prompt = f"""
    Please provide a concise business description for the table in two phrases or less. Do not mention the table name, where it is stored, or any technical details. Do not include any special characters.
    
    The table name is: {table_name}
    It is stored in the {schema_name} schema, in the {database_name} database.
    Here are the column descriptions of the table:
    {column_details}
    Focus on the business purpose and usage of the table.
    Focus solely on the business purpose and usage of the table. 
    Do not include any introductory phrases.
    """
    
    # Create the SQL query
    query = f"""SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3-70b', '{prompt.replace("'", "''")}') as response;"""

    # Debug: Print the query
    # Execute the query
    generated_description = session.sql(query).collect()
    return re.sub('[^A-Za-z0-9]+', ' ', generated_description[0]['RESPONSE']).replace('"', "").strip().replace('\n', ' ')


# Function to show the welcome page
def show_welcome_page():
    st.title("Cortex Analyst")
    st.markdown("Powered by Streamlit in Snowflake :snowflake:")

    st.markdown("""
### Description


The Cortex Analyst generator app simplifies and automates YAML files creation for [Snowflake's Cortex Analyst](https://docs.snowflake.com/LIMITEDACCESS/snowflake-cortex/cortex-analyst-overview#overview). 

Users can select databases, schemas, and tables, and the tool auto-fills the YAML structure with LLM-generated table description, column descriptions and relevant table info thanks to [Cortex LLM functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions). 

The generated files can be downloaded or uploaded to a stage for use in creating semantic models for Cortex Analyst. 

_While it quickly populates most required fields, some additional details could be added to get even better responses (such as: filters, verified_queries ...)_

#### Instructions

1. **Getting Started Page:**
   - Enter Semantic Name: Input the name of your semantic model.
   - Enter Description: Provide a detailed description of the semantic model.
   - Click on "Save Semantic Model Info" to save the details and proceed to the Table Definition page.

2. **Table Definition Page:**
   - Select Database: Choose the database from the dropdown menu.
   - Select Schema: Select the schema associated with the chosen database.
   - Select Table: Pick the table you want to include in the YAML file. 
   - Click on "Add Table to YAML" to add the selected table to the YAML structure. The YAML display will be updated accordingly. Table and column descriptions are generated using the *Llama3-70b* model via the [COMPLETE](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions#label-cortex-llm-complete) function, a feature of [Cortex LLM](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions).
   - Upload YAML to Stage: Once your tables are added, you can upload the generated YAML file by clicking on the "Upload to Stage" button.
   - You can also Download the YAML file to your local machine by clicking on the "Download YAML file" button.

3. **Chat with your data Page:**
   - Once the YAML file is uploaded, you can use Cortex Analyst's conversational interface to interact with selected tables, generating accurate SQL queries and interpretations.

4. **Reset Application:**
   - Click the "Reset" button to clear all saved data and reset the application to its initial state.

For further details, refer to the [Snowflake Documentation](https://docs.snowflake.com/LIMITEDACCESS/snowflake-cortex/semantic-model-spec#label-semantic-model-tips).

""")

st.image("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTGtsjtT26xLbvGO_eRAcJJ2drgv6wC9S7REQ&s")


def show_get_started_page():
    st.title("Getting Started")
    semantic_name = st.text_input("Enter Semantic Name")
    description = st.text_area("Enter Description of Semantic Model")
    if st.button("Save Semantic Model Info"):
        st.session_state['semantic_name'] = semantic_name
        st.session_state['description'] = description
        st.session_state['tables'] = []
        st.session_state['yaml_structure'] = {
            "name": semantic_name,
            "description": description,
            "tables": []
        }
        st.success("Semantic Model info saved successfully! Click on Table Definition on the navigation menu to finish YAML file creation")
        st.experimental_set_query_params(page="Table Definition")

# Function to show the table definition page
def show_table_definition_page():
    session = get_active_session()

    # Show databases and create a select box for the database selection
    databases_df = session.sql("SHOW DATABASES").collect()
    databases = [row['name'] for row in databases_df]
    database_selector = st.selectbox("Select Database", databases)

    # Show schemas based on the selected database and create a select box for schema selection
    schemas_df = session.sql(f"SHOW SCHEMAS IN DATABASE {database_selector}").collect()
    schemas = [row['name'] for row in schemas_df]
    schema_selector = st.selectbox("Select Schema", schemas)

    # Show tables based on the selected schema and create a select box for table selection
    tables_df = session.sql(f"SHOW TABLES IN {database_selector}.{schema_selector}").collect()
    tables = [row['name'] for row in tables_df]
    table_selector = st.selectbox("Select Table", tables)

    # Show stages based on the selected database / schema and create a select box for stage selection
    stages_df = session.sql(f"SHOW STAGES IN {database_selector}.{schema_selector}").collect()
    stages = [row['name'] for row in stages_df]
    stage_selector = st.selectbox("Select Stage", stages)
    
    # Display the current YAML structure
    yaml_template = {
        "name": "<name>",
        "description": "<string>",
        "tables": [
            {
                "name": "<name>",
                "description": "<string>",
                "base_table": {
                    "database": "<database>",
                    "schema": "<schema>",
                    "table": "<base table name>"
                },
                "dimensions": [
                    {
                        "name": "<name>",
                        "synonyms": ["<array of strings>"],
                        "description": "<string>",
                        "expr": "<SQL expression>",
                        "data_type": "<data type>"
                 #       "unique": False
                      #  "sample_values": ["<array of strings>"]
                    }
                ],
                "time_dimensions": [
                    {
                        "name": "date",
                        "synonyms": ["<array of strings>"],
                        "description": "<string>",
                        "expr": "date",
                        "data_type": "date"
               #         "unique": True
                    }
                ],
                "measures": [
                    {
                        "name": "<name>",
                        "synonyms": ["<array of strings>"],
                        "description": "<string>",
                        "expr": "<SQL expression>",
                        "data_type": "<data type>",
                        "default_aggregation": "<aggregate function>"
                    }
                ],
                "filters": [
                    {
                        "name": "<name>",
                        "synonyms": ["<array of strings>"],
                        "description": "<string>",
                        "expr": "<SQL expression>"
                    }
                ]
            }
        ]
    }
    
    if 'yaml_structure' not in st.session_state:
        st.session_state['yaml_structure'] = yaml_template
    
    # Add table to YAML structure
    if st.button("Add Table to YAML"):
        if len(st.session_state['tables']) < 100:
            st.session_state['tables'].append(table_selector)

            table_definition_df = session.sql(f"DESCRIBE TABLE {database_selector}.{schema_selector}.{table_selector}").collect()
            column_descriptions = session.create_dataframe(table_definition_df).to_pandas().to_dict('records')
            columns = [row['name'] for row in table_definition_df]
            data_types = [row['type'] for row in table_definition_df]
            unique_columns = [row['unique key']=='Y' for row in column_descriptions]
            ai_generated_column_descriptions = [generate_column_description(database_selector, schema_selector, table_selector, columns, column, column_description) for column, column_description in zip(columns, column_descriptions)]
            sample_values = [min_max_column(database_selector, schema_selector, table_selector, column) for column in columns ]

            # Add table definition to YAML structure
            table_entry = {
                "name": table_selector,
                "description": generate_table_description(database_selector, schema_selector, table_selector, column_descriptions).replace('\n', ' ').replace('\r', ' '),
                "base_table": {
                    "database": database_selector,
                    "schema": schema_selector,
                    "table": table_selector
                },
                "dimensions": [],
                "time_dimensions": [],
                "measures": []
            }

            # Check for time dimensions, dimension columns, and measure columns
            for column, data_type, column_description, unique_column, sample_value in zip(columns, data_types, ai_generated_column_descriptions, unique_columns, sample_values):
                if data_type.upper() in ["DATE", "DATETIME", "TIME", "TIMESTAMP", "TIMESTAMP_LTZ(9)", "TIMESTAMP_NTZ", "TIMESTAMP_TZ"]:
                    time_dimension_entry = {
                        "name": column,
                        "expr": column,
                        "description": column_description,
                  #      "unique": unique_column,
                        "data_type": data_type.upper()
                      #  "synonyms": ["<array of strings>"]
                    }
                    table_entry["time_dimensions"].append(time_dimension_entry)
                if data_type.upper() in ["VARCHAR(50)", "VARCHAR(16777216)", "VARCHAR(1)", "VARCHAR(10)", "VARCHAR(20)", "VARCHAR(30)", "CHAR", "CHARACTER", "STRING", "TEXT", "BINARY", "VARBINARY"]:
                    dimension_entry = {
                        "name": column,
                        "expr": column,
                        "description": column_description,
                        "data_type": data_type.upper(),
                   #     "unique": unique_column,
                    #    "synonyms": ["<array of strings>"]
                    }
                    table_entry["dimensions"].append(dimension_entry)
                if data_type.upper() in ["NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT", "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"]:
                    measure_entry = {
                        "name": column,
                        "expr": column,
                        "description": column_description,
                        "data_type": data_type.upper(),
                    }
                    table_entry["measures"].append(measure_entry)

            if len(table_entry["dimensions"]) == 0:
                del table_entry["dimensions"]
            if len(table_entry["measures"]) == 0:
                del table_entry["measures"]
            if len(table_entry["time_dimensions"]) == 0:
                del table_entry["time_dimensions"]
            st.session_state['yaml_structure']['tables'].append(table_entry)
            yaml_str = yaml.dump(st.session_state['yaml_structure'], sort_keys=False, indent=2, width=900)
            st.session_state['yaml_str'] = yaml_str  # Save to session state

    # Display the updated YAML structure
    st.code(st.session_state.get('yaml_str', yaml.dump(yaml_template, sort_keys=False, indent=2, width=900)), language='yaml')
    
    # Create a download button for the YAML file
    yaml_bytes = io.BytesIO(st.session_state.get('yaml_str', '').encode('utf-8'))
    
    st.download_button(
        label="Download YAML file",
        data=yaml_bytes,
        file_name="semantic_model.yaml",
        mime="text/plain"
    )
    
    st.button('Upload to Stage',         
              on_click = upload_to_stage,
              args = [database_selector, schema_selector, yaml_bytes, stage_selector]
             )
    
# Function to reset the app

def reset_app():
    st.title("Reset Application")
    st.warning("Are you sure you want to reset the application? This will clear all saved data.")
    
    if st.button("Reset"):
        for key in st.session_state.keys():
            del st.session_state[key]
        st.experimental_set_query_params(page="Welcome")
        st.success("Application has been reset. Please go to the Welcome page.")

# Main function to control the app
    
def main():
    st.sidebar.title("✨ Cortex Analyst ✨")
    st.sidebar.markdown("*Get instant and accurate insights on your Snowflake data in less than 2 minutes.*")

    st.sidebar.markdown((
    """
    With Cortex Analyst, business users can ask questions in natural language and receive direct answers without writing SQL. 
    
    This app automates the generation of semantic models using *[Cortex LLM](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions)*, accelerating the delivery of high-precision, self-serve conversational analytics.
    """
))

    st.sidebar.subheader("🔎 Navigation")

    page = st.sidebar.radio("Go to", ["Welcome", "Getting Started", "Table Definition", 'Chat with Your Data',  "Reset"])

    if page == "Welcome":
        show_welcome_page()
    elif page == "Getting Started":
        show_get_started_page()
    elif page == "Table Definition":
        show_table_definition_page()
    elif page == 'Chat with Your Data':
        show_cortex_analyst_page()
    elif page == "Reset":
        reset_app()

    st.sidebar.markdown('''<hr>''', unsafe_allow_html=True)
    st.sidebar.caption("More documentation on [Cortex Analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)")

# Auto-navigation based on session state
    if 'page' in st.session_state and st.session_state['page'] == "Table Definition":
        show_table_definition_page()

if __name__ == "__main__":
    main()