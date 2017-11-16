module BinDicts
using Libz

import ..BackendBase: AbstractBigArrayBackend, get_info, get_scale_name 
export BinDict, get_info, get_scale_name  

"""
BinDict should follow the format of neuroglancer precomputed. the dataset structure will be the same with neuroglancer, which means copying dataset directly to the cloud should make it available immediately.
"""
struct BinDict <: AbstractBigArrayBackend  
    path::String
end 

function get_path(self::BinDict)
    self.path 
end 

function get_info( self::BinDict )
    readstring( joinpath( get_path(self), "../info" ) )
end 

function get_scale_name( self::BinDict ) 
    basename( strip(get_path(self), '/') )
end 

function Base.getindex( self::BinDict, key::AbstractString)
    f = open( joinpath( get_path(self), key ))
    data = read(f)
    close(f)
    Libz.inflate(data)
end 

function Base.setindex!( self::BinDict, value::Array, key::AbstractString )
    data = Libz.deflate(reinterpret(UInt8, value[:]))
    fileName = joinpath( get_path(self), key )
    f = open(fileName, "w")
    write(f, data)
    close(f)
end 

end # module
