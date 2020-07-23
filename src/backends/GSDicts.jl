module GSDicts

using HTTP
using GoogleCloud
using JSON
import BigArrays.BackendBase: AbstractBigArrayBackend, get_info, get_scale_name 

export GSDict, get_credential_filename

const GZIP_MAGIC_NUMBER = UInt8[0x1f, 0x8b, 0x08]

struct GSDict <: AbstractBigArrayBackend
    bucketName  	::String
    keyPrefix   	::String
    session	::GoogleCloud.session.GoogleSession
end

"""
    GSDict( path::String; gzip::Bool = GZIP )
construct an associative datastructure based on Google Cloud Storage
format.
"""
function GSDict(path::String; 
                credentialFileName = get_credential_filename())
    @assert startswith(path, "gs://")
    bucketName, keyPrefix = splitgs(path)
    bucketName = replace(bucketName, "gs://"=>"")
    
    session = GoogleSession( credentialFileName, ["devstorage.full_control"]) 
    set_session!(storage, session)    # storage is the API root, exported from GoogleCloud.jl
    GSDict( bucketName, keyPrefix, session )
end

##################### properties ##############################
function get_info( self::GSDict )
    storage(:Object, :get, self.bucketName, joinpath(self.keyPrefix, "info"))
end

function get_scale_name(self::GSDict)
    basename( self.keyPrefix )
end 
"""
get the voxel offset in info
"""
function get_voxel_offset(h::GSDict)
    h.configDict[:offset]
end 

###################### Base functions #########################
@inline function Base.delete!( d::GSDict, key::String )
    storage(:Object, :delete, d.bucketName, joinpath(d.keyPrefix, key))
end

function Base.setindex!( d::GSDict, value::Vector{UInt8}, key::AbstractString )
	#authorize( d.googleSession )
    if all(value[1:3] .== GZIP_MAGIC_NUMBER)
        return storage(:Object, :insert, d.bucketName; 
                        name=joinpath(d.keyPrefix, key), 
                        data=value, gzip=true, 
                        content_type="application/octet-stream", 
                        fields="")
    else 
        response = storage(:Object, :insert, d.bucketName; 
                        name=joinpath(d.keyPrefix, key), 
                        data=value, gzip=false, 
                        content_type="application/octet-stream", 
                        fields="")
    end 
end

function Base.setindex!( d::GSDict, value::Dict, key::AbstractString )
	#authorize( d.googleSession )
    response = storage(:Object, :insert, d.bucketName; 
                       name=joinpath(d.keyPrefix, key), 
                       data=JSON.json(value), gzip=false, 
                       content_type="application/json")
end

function Base.setindex!( d::GSDict, value::AbstractString, key::AbstractString )
	#authorize( d.googleSession )
    response = storage(:Object, :insert, d.bucketName; 
                       name=joinpath(d.keyPrefix, key), 
                       data=value, gzip=false, 
                       content_type="text/plain")
end

function Base.getindex( d::GSDict, key::AbstractString)
    try
        return storage(:Object, :get, d.bucketName, joinpath(d.keyPrefix, key))
    catch err 
        if isa(err, HTTP.ExceptionRequest.StatusError) && err.status==404
            # @show d.bucketName, d.keyPrefix
            @warn "NoSuchKey in Google Cloud Storage: $(key)"
            return nothing
        elseif isa(err, UndefVarError)
            return nothing
        else
            println("get an unknown error: ", err)
            println("error type is: ", typeof(err))
            rethrow
        end 
    end 
end

@inline function Base.getindex(d::GSDict, key::Symbol)
    d[string(key)]
end

function Base.keys( d::GSDict )
    ds = storage(:Object, :list, d.bucketName; prefix=d.keyPrefix, fields="items(name)")
    ret = Vector{String}()
    for i in eachindex(ds)
        chunkFileName = replace(ds[i][:name], "$(rstrip(d.keyPrefix, '/'))/" => "" )
        push!(ret, chunkFileName)
    end
    return ret
end

function Base.haskey( d::GSDict, key::String )
    @warn("this haskey function will download the object rather than just check whether it exist or not")
    response = storage(:Object, :get, d.bucketName, joinpath(d.keyPrefix, key))
    !GoogleCloud.api.iserror(response)
end

################### utility functions #################

function get_credential_filename()
    if isfile(expanduser("~/.google_credentials.json"))
        return expanduser("~/.google_credentials.json")
    elseif isfile(joinpath(dirname(@__FILE__), "../.google_credentials.json"))
        return joinpath(dirname(@__FILE__), "../.google_credentials.json")
    elseif isfile(expanduser("~/.cloudvolume/secrets/google-secret.json"))
        return expanduser("~/.cloudvolume/secrets/google-secret.json")
    elseif isfile("/secrets/google-secret.json")
        return "/secrets/google-secret.json"
    else
        # to enable building of this package 
        @warn("google credential file is not in default place!")
        return nothing
    end
end

"""
    split gs path to bucket name and key
"""
function splitgs( path::String )
    path = replace(path, "gs://"=>"")
    bucketName, key = split(path, "/", limit=2)
    key = rstrip(key, '/')
    return String(bucketName), String(key)
end

end # end of module
