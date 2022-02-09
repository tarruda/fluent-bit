
local orig_schema

function wrap_record(tag, timestamp, record)
    if orig_schema == nil then
        local fobj = io.open('customer.json', 'rb')
        orig_schema = fobj:read('*a')
        fobj:close()
    end
    return 1, timestamp, {
        metadata = '',
        avro_schema = orig_schema,
        max_size = 1024,
        payload = record
    }
end
