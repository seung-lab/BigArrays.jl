module BackendBase
export AbstractBigArrayBackend, get_info, get_scale_name 
abstract type AbstractBigArrayBackend <: Associative{String, Any} end 

function get_info end 
function get_scale_name end 

end # module
