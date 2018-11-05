module BinDicts
#using Libz

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
    read( joinpath( get_path(self), "../info" ) , String)
end 

function get_scale_name( self::BinDict ) 
    basename( rstrip(get_path(self), '/') )
end 

function Base.getindex( self::BinDict, key::AbstractString)
    open( joinpath( get_path(self), key )) do f
        return read(f)
        #Libz.inflate(data)
    end
end 

function Base.setindex!( self::BinDict, value::Array, key::AbstractString )
    data = reinterpret(UInt8, value[:])
    fileName = joinpath( get_path(self), key )
    write(fileName, data)
end 

function Base.haskey( self::BinDict, key::AbstractString )
    joinpath( get_path(self), key ) |> isfile 
end


end # module
