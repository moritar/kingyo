# import modules
using PyCall, DataFrames, DataFramesMeta, Conda, CSVFiles, Dates
using RCall
using Plots, StatsPlots
Conda.add("ciso8601")
Conda.add("influxdb-client")
###
# load influx DB settings

INFLUX_HOST = "150.65.34.25:8086"
INFLUX_TOKEN = "my-super-secret-auth-token"
INFLUX_ORG = "test"
INFLUX_BUCKET = "sensor_data"

influxdb_client = pyimport("influxdb_client")
client = influxdb_client.InfluxDBClient(
    url = INFLUX_HOST,
    token = INFLUX_TOKEN,
    org = INFLUX_ORG,
)

query_api = client.query_api()

result = query_api.query_csv("""
from(bucket:"$(INFLUX_BUCKET)") |>
    range(start: -30d)
""")

sensor_string_list = []
for d in result
    push!(sensor_string_list, d)
end

matrixed_data = map(d -> reshape(d, 1, length(d)), sensor_string_list[5:end-1]) |> d ->
    vcat(d...)

df_sensor_data = DataFrame(matrixed_data, sensor_string_list[4])

function get_aqualium_no(topic::String)
    split(topic, "/")[2]
end

df = @linq df_sensor_data |>
    select($(Symbol.(filter(d -> !(d in ["_start", "_stop", "", "table", "result"]), names(df_sensor_data))))) |>
    transform(:aqualium_no = get_aqualium_no.(:topic)) |>
    transform(:_time = DateTime.(:_time, dateformat"yyyy-mm-ddTHH:MM:SSZ")) |>
    orderby(:_time)
###

measurement_point_list = unique(df.topic)
df_grouped = @linq df |>
    groupby(:_time)

combined_df_base = [
        [
            first(unique(cur_df._time)),
            map(x -> begin
                if (x in cur_df.topic)
                    value_df = @linq cur_df |>
                        where(:topic .== x)
                    value_df._value[1]
                else
                    nothing
                end
            end, unique(df.topic))...
        ]
    for (index, cur_df) in enumerate(df_grouped)
]

base_df = permutedims(hcat(combined_df_base...))
combine_df_symbols = Symbol.(["time", replace.(unique(df.topic), "/" => "_")...])
combined_df = DataFrame(
        base_df,
        combine_df_symbols
    )

parse_float(value) = begin
    if (typeof(value) == String)
        tryparse(Float64, value)
    else
        missing
    end
end

parse_int(value) = begin
    if (typeof(value) == String)
        tryparse(Int, value)
    else
        missing
    end
end

parse_dateTime(value) = typeof(value) == DateTime ? value : missing

typed_combined_df = @linq combined_df |> 
    transform(
        :time = parse_dateTime.(:time),
        :aqualium_1_a_brightness = parse_float.(:aqualium_1_a_brightness),
        :aqualium_1_b_brightness = parse_float.(:aqualium_1_b_brightness),
        :aqualium_2_a_brightness = parse_float.(:aqualium_2_a_brightness),
        :aqualium_1_b_dissolved_oxy = parse_float.(:aqualium_1_b_dissolved_oxy),
        :aqualium_1_a_humidity_and_temperature = parse_float.(:aqualium_1_a_humidity_and_temperature),
        :aqualium_2_a_humidity_and_temperature = parse_float.(:aqualium_2_a_humidity_and_temperature),
        :aqualium_1_b_ph = parse_float.(:aqualium_1_b_ph),
        :aqualium_1_a_tds = parse_float.(:aqualium_1_a_tds),
        :aqualium_2_a_tds = parse_float.(:aqualium_2_a_tds),
        :aqualium_1_a_water_level = parse_float.(:aqualium_1_a_water_level),
        :aqualium_2_a_water_level = parse_float.(:aqualium_2_a_water_level),
        :aqualium_1_b_water_temp_bottom = parse_float.(:aqualium_1_b_water_temp_bottom),
        :aqualium_1_a_water_temp_upper = parse_float.(:aqualium_1_a_water_temp_upper),
        :aqualium_2_a_water_temp_upper = parse_float.(:aqualium_2_a_water_temp_upper),
    )

for i in 2:length(typed_combined_df[1, :])
    typed_combined_df[!, i] = parse_float.(typed_combined_df[!, i])
end

@df typed_combined_df corrplot(cols(2:10), grid = false)

plot(typed_combined_df.time, [typed_combined_df[!, i] for i in 2:19])
