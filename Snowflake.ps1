#
# Microsoft SQL.ps1 - IDM System PowerShell Script for Microsoft SQL Server.
#
# Any IDM System PowerShell Script is dot-sourced in a separate PowerShell context, after
# dot-sourcing the IDM Generic PowerShell Script '../Generic.ps1'.
#


$Log_MaskableKeys = @(
    'password'
)


#
# System functions
#

function Idm-SystemInfo {
    param (
        # Operations
        [switch] $Connection,
        [switch] $TestConnection,
        [switch] $Configuration,
        # Parameters
        [string] $ConnectionParams
    )

    Log info "-Connection=$Connection -TestConnection=$TestConnection -Configuration=$Configuration -ConnectionParams='$ConnectionParams'"
    
    if ($Connection) {
        @(
            @{
                name = 'server'
                type = 'textbox'
                label = 'Server'
                description = 'Name of Snowflake server'
                value = ''
            }
            @{
                name = 'database'
                type = 'textbox'
                label = 'Database'
                description = 'Name of Snowflake database'
                value = ''
            }
            @{
                name = 'schema'
                type = 'textbox'
                label = 'Schema'
                description = 'Name of Snowflake schema'
                value = ''
            }
             @{
                name = 'role'
                type = 'textbox'
                label = 'Role'
                description = 'Name of Snowflake Role'
                value = ''
            }
             @{
                name = 'warehouse'
                type = 'textbox'
                label = 'Warehouse'
                description = 'Name of Snowflake Warehouse'
                value = ''
            }
            @{
                name = 'use_svc_account_creds'
                type = 'checkbox'
                label = 'Use credentials of service account'
                value = $true
            }
            @{
                name = 'username'
                type = 'textbox'
                label = 'Username'
                label_indent = $true
                description = 'User account name to access Microsoft SQL server'
                value = ''
                hidden = 'use_svc_account_creds'
            }
            @{
                name = 'password'
                type = 'textbox'
                password = $true
                label = 'Password'
                label_indent = $true
                description = 'User account password to access Microsoft SQL server'
                value = ''
                hidden = 'use_svc_account_creds'
            }
            @{
                name = 'nr_of_sessions'
                type = 'textbox'
                label = 'Max. number of simultaneous sessions'
                description = ''
                value = 5
            }
            @{
                name = 'sessions_idle_timeout'
                type = 'textbox'
                label = 'Session cleanup idle time (minutes)'
                description = ''
                value = 30
            }
        )
    }

    if ($TestConnection) {
        Open-SnowflakeConnection $ConnectionParams
    }

    if ($Configuration) {
        @()
    }

    Log info "Done"
}


function Idm-OnUnload {
    Close-SnowflakeConnection
}


#
# CRUD functions
#

$ColumnsInfoCache = @{}

$SqlInfoCache = @{}


function Fill-SqlInfoCache {
    param (
        [switch] $Force
    )

    if (!$Force -and $Global:SqlInfoCache.Ts -and ((Get-Date) - $Global:SqlInfoCache.Ts).TotalMilliseconds -le [Int32]600000) {
        return
    }

    $pk_command = New-SnowflakeCommand "show primary keys"
    $pkResult = Invoke-SnowflakeCommand $pk_command
    $primaryKeys = New-Object System.Collections.ArrayList
    
    foreach($key in $pkResult) {
        $value = "{0}.{1}.{2}.{3}" -f $key.database_name, $key.schema_name,$key.table_name,$key.column_name
        [void]$primaryKeys.Add($value)
    }

    # Refresh cache
    $sql_command = New-SnowflakeCommand "
        SELECT  CONCAT(st.TABLE_SCHEMA,'.',st.TABLE_NAME,'') full_object_name,
                st.TABLE_CATALOG,
                st.TABLE_SCHEMA,
                st.TABLE_NAME,
                st.TABLE_TYPE object_type,
                sc.COLUMN_NAME,
                CASE WHEN sc.is_identity = 'YES' THEN 1 ELSE 0 END is_identity,
                0 is_computed,
                CASE WHEN sc.is_nullable = 'YES' THEN 1 ELSE 0 END is_nullable
                
        FROM (
                        SELECT TABLE_CATALOG, TABLE_NAME, TABLE_SCHEMA,TABLE_TYPE
                        from information_schema.tables
                        UNION
                        SELECT TABLE_CATALOG, TABLE_NAME, TABLE_SCHEMA, 'VIEW' TABLE_TYPE
                        from information_schema.views
        ) st
        INNER JOIN information_schema.columns sc ON sc.TABLE_CATALOG = st.TABLE_CATALOG AND sc.TABLE_NAME = st.TABLE_NAME AND sc.TABLE_SCHEMA = st.TABLE_SCHEMA
        WHERE st.TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
        ORDER BY full_object_name, column_name
    "

    $objects = New-Object System.Collections.ArrayList
    $object = @{}

    # Process in one pass
    Invoke-SnowflakeCommand $sql_command | ForEach-Object {
        if ($_.full_object_name -ne $object.full_name) {
            if ($object.full_name -ne $null) {
                $objects.Add($object) | Out-Null
            }
            
            $object = @{
                full_name = $_.full_object_name
                type      = $_.object_type
                columns   = New-Object System.Collections.ArrayList
            }
        }
        $pkValue = "{0}.{1}.{2}.{3}" -f $_.TABLE_CATALOG, $_TABLE_SCHEMA,$_.TABLE_NAME,$_.COLUMN_NAME
        $object.columns.Add(@{
            name           = $_.column_name
            is_primary_key = if($primaryKeys.contains($pkValue)) { 1 } else { 0 }
            is_identity    = $_.is_identity
            is_computed    = $_.is_computed
            is_nullable    = $_.is_nullable
        }) | Out-Null
    }

    if ($object.full_name -ne $null) {
        $objects.Add($object) | Out-Null
    }

    Dispose-SnowflakeCommand $sql_command

    $Global:SqlInfoCache.Objects = $objects
    $Global:SqlInfoCache.Ts = Get-Date
}


function Idm-Dispatcher {
    param (
        # Optional Class/Operation
        [string] $Class,
        [string] $Operation,
        # Mode
        [switch] $GetMeta,
        # Parameters
        [string] $SystemParams,
        [string] $FunctionParams
    )

    Log info "-Class='$Class' -Operation='$Operation' -GetMeta=$GetMeta -SystemParams='$SystemParams' -FunctionParams='$FunctionParams'"

    if ($Class -eq '') {

        if ($GetMeta) {
            #
            # Get all tables and views in database
            #

            Open-SnowflakeConnection $SystemParams

            Fill-SqlInfoCache -Force

            #
            # Output list of supported operations per table/view (named Class)
            #

            @(
                foreach ($object in $Global:SqlInfoCache.Objects) {
                    $primary_keys = $object.columns | Where-Object { $_.is_primary_key } | ForEach-Object { $_.name }

                    if ($object.type -ne 'Table') {
                        # Non-tables only support 'Read'
                        [ordered]@{
                            Class = $object.full_name
                            Operation = 'Read'
                            'Source type' = $object.type
                            'Primary key' = $primary_keys -join ', '
                            'Supported operations' = 'R'
                        }
                    }
                    else {
                        [ordered]@{
                            Class = $object.full_name
                            Operation = 'Create'
                        }

                        [ordered]@{
                            Class = $object.full_name
                            Operation = 'Read'
                            'Source type' = $object.type
                            'Primary key' = $primary_keys -join ', '
                            'Supported operations' = "CR$(if ($primary_keys) { 'UD' } else { '' })"
                        }

                        if ($primary_keys) {
                            # Only supported if primary keys are present
                            [ordered]@{
                                Class = $object.full_name
                                Operation = 'Update'
                            }

                            [ordered]@{
                                Class = $object.full_name
                                Operation = 'Delete'
                            }
                        }
                    }
                }
            )

        }
        else {
            # Purposely no-operation.
        }

    }
    else {

        if ($GetMeta) {
            #
            # Get meta data
            #

            Open-SnowflakeConnection $SystemParams

            Fill-SqlInfoCache

            $columns = ($Global:SqlInfoCache.Objects | Where-Object { $_.full_name -eq $Class }).columns

            switch ($Operation) {
                'Create' {
                    @{
                        semantics = 'create'
                        parameters = @(
                            $columns | ForEach-Object {
                                @{
                                    name = $_.name;
                                    allowance = if ($_.is_identity -or $_.is_computed) { 'prohibited' } elseif (! $_.is_nullable) { 'mandatory' } else { 'optional' }
                                }
                            }
                        )
                    }
                    break
                }

                'Read' {
                    @(
                        @{
                            name = 'select_distinct'
                            type = 'checkbox'
                            label = 'Distinct Rows'
                            description = 'Apply Distinct to select'
                            value = $false
                        }
                        @{
                            name = 'where_clause'
                            type = 'textbox'
                            label = 'Filter (SQL where-clause)'
                            description = 'Applied SQL where-clause'
                            value = ''
                        }
                        @{
                            name = 'selected_columns'
                            type = 'grid'
                            label = 'Include columns'
                            description = 'Selected columns'
                            table = @{
                                rows = @($columns | ForEach-Object {
                                    @{
                                        name = $_.name
                                        config = @(
                                            if ($_.is_primary_key) { 'Primary key' }
                                            if ($_.is_identity)    { 'Generated' }
                                            if ($_.is_computed)    { 'Computed' }
                                            if ($_.is_nullable)    { 'Nullable' }
                                        ) -join ' | '
                                    }
                                })
                                settings_grid = @{
                                    selection = 'multiple'
                                    key_column = 'name'
                                    checkbox = $true
                                    filter = $true
                                    columns = @(
                                        @{
                                            name = 'name'
                                            display_name = 'Name'
                                        }
                                        @{
                                            name = 'config'
                                            display_name = 'Configuration'
                                        }
                                    )
                                }
                            }
                            value = @($columns | ForEach-Object { $_.name })
                        }
                    )
                    break
                }

                'Update' {
                    @{
                        semantics = 'update'
                        parameters = @(
                            $columns | ForEach-Object {
                                @{
                                    name = $_.name;
                                    allowance = if ($_.is_primary_key) { 'mandatory' } else { 'optional' }
                                }
                            }
                            @{
                                name = '*'
                                allowance = 'prohibited'
                            }
                        )
                    }
                    break
                }

                'Delete' {
                    @{
                        semantics = 'delete'
                        parameters = @(
                            $columns | ForEach-Object {
                                if ($_.is_primary_key) {
                                    @{
                                        name = $_.name
                                        allowance = 'mandatory'
                                    }
                                }
                            }
                            @{
                                name = '*'
                                allowance = 'prohibited'
                            }
                        )
                    }
                    break
                }
            }

        }
        else {
            #
            # Execute function
            #

            Open-SnowflakeConnection $SystemParams

            if (! $Global:ColumnsInfoCache[$Class]) {
                Fill-SqlInfoCache

                $columns = ($Global:SqlInfoCache.Objects | Where-Object { $_.full_name -eq $Class }).columns

                $Global:ColumnsInfoCache[$Class] = @{
                    primary_keys = @($columns | Where-Object { $_.is_primary_key } | ForEach-Object { $_.name })
                    identity_col = @($columns | Where-Object { $_.is_identity    } | ForEach-Object { $_.name })[0]
                }
            }

            $primary_keys = $Global:ColumnsInfoCache[$Class].primary_keys
            $identity_col = $Global:ColumnsInfoCache[$Class].identity_col

            $function_params = ConvertFrom-Json2 $FunctionParams

            # Replace $null by [System.DBNull]::Value
            $keys_with_null_value = @()
            foreach ($key in $function_params.Keys) { if ($function_params[$key] -eq $null) { $keys_with_null_value += $key } }
            foreach ($key in $keys_with_null_value) { $function_params[$key] = [System.DBNull]::Value }

            $sql_command = New-SnowflakeCommand

            $projection = if ($function_params['selected_columns'].count -eq 0) { '*' } else { @($function_params['selected_columns'] | ForEach-Object { "[$_]" }) -join ', ' }

            if ($function_params['select_distinct']) { $projection = "DISTINCT $($projection)" }

            switch ($Operation) {
                'Create' {
                    $filter = if ($identity_col) {
                                  "[$identity_col] = SCOPE_IDENTITY()"
                              }
                              elseif ($primary_keys) {
                                  @($primary_keys | ForEach-Object { "[$_] = $(AddParam-SnowflakeCommand $sql_command $function_params[$_])" }) -join ' AND '
                              }
                              else {
                                  @($function_params.Keys | ForEach-Object { "[$_] = $(AddParam-SnowflakeCommand $sql_command $function_params[$_])" }) -join ' AND '
                              }

                    $sql_command.CommandText = "
                        INSERT INTO $Class (
                            $(@($function_params.Keys | ForEach-Object { "[$_]" }) -join ', ')
                        )
                        VALUES (
                            $(@($function_params.Keys | ForEach-Object { AddParam-SnowflakeCommand $sql_command $function_params[$_] }) -join ', ')
                        );
                        SELECT TOP(1)
                            $projection
                        FROM
                            $Class
                        WHERE
                            $filter
                    "
                    break
                }

                'Read' {
                    $filter = if ($function_params['where_clause'].length -eq 0) { '' } else { " WHERE $($function_params['where_clause'])" }

                    $sql_command.CommandText = "
                        SELECT
                            $projection
                        FROM
                            $Class$filter
                    "
                    break
                }

                'Update' {
                    $filter = @($primary_keys | ForEach-Object { "[$_] = $(AddParam-SnowflakeCommand $sql_command $function_params[$_])" }) -join ' AND '

                    $sql_command.CommandText = "
                        UPDATE TOP(1)
                            $Class
                        SET
                            $(@($function_params.Keys | ForEach-Object { if ($_ -notin $primary_keys) { "[$_] = $(AddParam-SnowflakeCommand $sql_command $function_params[$_])" } }) -join ', ')
                        WHERE
                            $filter;
                        SELECT TOP(1)
                            $(@($function_params.Keys | ForEach-Object { "[$_]" }) -join ', ')
                        FROM
                            $Class
                        WHERE
                            $filter
                    "
                    break
                }

                'Delete' {
                    $filter = @($primary_keys | ForEach-Object { "[$_] = $(AddParam-SnowflakeCommand $sql_command $function_params[$_])" }) -join ' AND '

                    $sql_command.CommandText = "
                        DELETE TOP(1)
                            $Class
                        WHERE
                            $filter
                    "
                    break
                }
            }

            if ($sql_command.CommandText) {
                $deparam_command = DeParam-SnowflakeCommand $sql_command

                LogIO info ($deparam_command -split ' ')[0] -In -Command $deparam_command

                if ($Operation -eq 'Read') {
                    # Streamed output
                    Log debug 'read'
                    Log debug $sql_command
                    Invoke-SnowflakeCommand $sql_command $deparam_command
                }
                else {
                    # Log output
                    $rv = Invoke-SnowflakeCommand $sql_command $deparam_command | ForEach-Object { $_ }
                    LogIO info ($deparam_command -split ' ')[0] -Out $rv

                    $rv
                }
            }

            Dispose-SnowflakeCommand $sql_command

        }

    }

    Log info "Done"
}


#
# Helper functions
#

function New-SnowflakeCommand {
    param (
        [string] $CommandText
    )

    New-Object System.Data.Odbc.OdbcCommand($CommandText, $Global:SnowflakeConnection)
}


function Dispose-SnowflakeCommand {
    param (
        [System.Data.Odbc.OdbcCommand] $SqlCommand
    )

    $SqlCommand.Dispose()
}


function AddParam-SnowflakeCommand {
    param (
        [System.Data.Odbc.OdbcCommand] $SqlCommand,
        $Param
    )

    $param_name = "@param$($SqlCommand.Parameters.Count)_"
    $param_value = if ($Param -isnot [system.array]) { $Param } else { $Param | ConvertTo-Json -Compress -Depth 32 }

    $SqlCommand.Parameters.AddWithValue($param_name, $param_value) | Out-Null

    return $param_name
}


function DeParam-SnowflakeCommand {
    param (
        [System.Data.Odbc.OdbcCommand] $SqlCommand
    )

    $deparam_command = $SqlCommand.CommandText

    foreach ($p in $SqlCommand.Parameters) {
        $value_txt = 
            if ($p.Value -eq [System.DBNull]::Value) {
                'NULL'
            }
            else {
                switch ($p.SqlDbType) {
                    { $_ -in @(
                        [System.Data.SqlDbType]::Char
                        [System.Data.SqlDbType]::Date
                        [System.Data.SqlDbType]::DateTime
                        [System.Data.SqlDbType]::DateTime2
                        [System.Data.SqlDbType]::DateTimeOffset
                        [System.Data.SqlDbType]::NChar
                        [System.Data.SqlDbType]::NText
                        [System.Data.SqlDbType]::NVarChar
                        [System.Data.SqlDbType]::Text
                        [System.Data.SqlDbType]::Time
                        [System.Data.SqlDbType]::VarChar
                        [System.Data.SqlDbType]::Xml
                    )} {
                        "'" + $p.Value.ToString().Replace("'", "''") + "'"
                        break
                    }
        
                    default {
                        $p.Value.ToString().Replace("'", "''")
                        break
                    }
                }
            }

        $deparam_command = $deparam_command.Replace($p.ParameterName, $value_txt)
    }

    # Make one single line
    @($deparam_command -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }) -join ' '
}


function Invoke-SnowflakeCommand {
    param (
        [System.Data.Odbc.OdbcCommand] $SqlCommand,
        [string] $DeParamCommand
    )

    # Streaming
    function Invoke-SnowflakeCommand-ExecuteReader {
        param (
            [System.Data.Odbc.OdbcCommand] $SqlCommand
        )

        $data_reader = $SqlCommand.ExecuteReader()
        $column_names = @($data_reader.GetSchemaTable().ColumnName)

        if ($column_names) {

            $hash_table = [ordered]@{}

            foreach ($column_name in $column_names) {
                $hash_table[$column_name] = ""
            }

            $obj = New-Object -TypeName PSObject -Property $hash_table

            # Read data
            while ($data_reader.Read()) {
                foreach ($column_name in $column_names) {
                    $obj.$column_name = if ($data_reader[$column_name] -is [System.DBNull]) { $null } else { $data_reader[$column_name] }
                }

                # Output data
                $obj
            }

        }

        $data_reader.Close()
    }

    if (! $DeParamCommand) {
        $DeParamCommand = DeParam-SnowflakeCommand $SqlCommand
    }

    Log debug $DeParamCommand

    try {
        Invoke-SnowflakeCommand-ExecuteReader $SqlCommand
    }
    catch {
        Log error "Failed: $_"
        Write-Error $_
    }

    Log debug "Done"
}


function Open-SnowflakeConnection {
    param (
        [string] $ConnectionParams
    )

    $connection_params = ConvertFrom-Json2 $ConnectionParams

    $connection_string = ("Driver={{SnowflakeDSIIDriver}};Server={0};Database={1};UID={2};PWD={3};warehouse={4};role={5};schema={6}" -f $connection_params.server, $connection_params.database, $connection_params.username, $connection_params.password, $connection_params.warehouse, $connection_params.role, $connection_params.schema)
    Log debug $connection_string
    if ($Global:SnowflakeConnection -and $connection_string -ne $Global:SnowflakeConnectionString) {
        Log info "SnowflakeConnection connection parameters changed"
        Close-SnowflakeConnection
    }

    if ($Global:SnowflakeConnection -and $Global:SnowflakeConnection.State -ne 'Open') {
        Log warn "SnowflakeConnection State is '$($Global:SnowflakeConnection.State)'"
        Close-SnowflakeConnection
    }

    if ($Global:SnowflakeConnection) {
        #Log debug "Reusing SnowflakeConnection"
    }
    else {
        Log info "Opening SnowflakeConnection '$connection_string'"

        try {
            $connection = New-Object System.Data.Odbc.OdbcConnection
            $connection.ConnectionString = $connection_string
            $connection.Open()

            $Global:SnowflakeConnection       = $connection
            $Global:SnowflakeConnectionString = $connection_string

            $Global:ColumnsInfoCache = @{}
            $Global:SqlInfoCache = @{}
        }
        catch {
            Log error "Failed: $_"
            Write-Error $_
        }

        Log info "Done"
    }
}


function Close-SnowflakeConnection {
    if ($Global:SnowflakeConnection) {
        Log info "Closing SnowflakeConnection"

        try {
            $Global:SnowflakeConnection.Close()
            $Global:SnowflakeConnection = $null
        }
        catch {
            # Purposely ignoring errors
        }

        Log info "Done"
    }
}
