module BigQueryPipe
export gbq_query_df

using JSON3
using JSONTables
using DataFrames


struct GBQException <: Exception 
    var::String
end

Base.showerror(io::IO, e::GBQException) = print(io, e.var)

function _basic_type_converter(df)
    float_col = []
    int_col = []
    for i in names(df)
        try
            if eltype(tryparse.(Int, df[!, i])) == Int
                int_col = vcat(int_col, i)
            elseif eltype(tryparse.(Float64, df[!, i])) == Float64
                float_col = vcat(float_col, i)
            end
        catch
        end
    end
    try
        if length(int_col) > 0
            transform!(df, int_col .=> ByRow(x -> parse(Int, x)) .=> int_col)
        end
        if length(float_col) > 0
            transform!(df, float_col .=> ByRow(x -> parse(Float64, x)) .=> float_col)
        end
    catch e
        println(e)
    end
    return df
end

function _gbq_parse_df(response)
    #df = reduce(vcat, DataFrame.(response))
    try
        df = DataFrame(jsontable(response))
        _basic_type_converter(df)
        return df
    catch
        println(e)
    end
end

# run a query and export to a DataFrame

# Test that bq command line tool is working with the following script
# bq query --format=json --quiet=true --use_legacy_sql=false --max_rows=10 'SELECT COUNT(1) AS counter FROM `dataset.table`' 
function gbq_query_df(query; use_legacy_sql=false, quiet=true, max_rows=100000000)
    response = JSON3.read(read(`bq query --format=json --quiet="$quiet" --use_legacy_sql="$use_legacy_sql" --max_rows="$max_rows" $query`))
    return _gbq_parse_df(response)
end

end
