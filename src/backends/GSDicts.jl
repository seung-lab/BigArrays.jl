module GSDicts

using GoogleCloud
using JSON
import BigArrays.BackendBase: AbstractBigArrayBackend, get_info, get_scale_name 

export GSDict, get_credential_filename

const GZIP = true

struct GSDict <: AbstractBigArrayBackend
    kvStore     	::KeyStore
    bucketName  	::String
    keyPrefix   	::String
    session	::GoogleCloud.session.GoogleSession
end

"""
    GSDict( path::String; gzip::Bool = GZIP )
construct an associative datastructure based on Google Cloud Storage
format.
"""
function GSDict( path::String; gzip::Bool = GZIP, 
                    credentialFileName = get_credential_filename(),
                    valueType::DataType = Vector{UInt8})
    bucketName, keyPrefix = splitgs(path)
    bucketName = replace(bucketName, "gs://"=>"")
    
    session = GoogleSession( credentialFileName, ["devstorage.full_control"]) 
    set_session!(storage, session)    # storage is the API root, exported from GoogleCloud.jl
    kvStore = KeyStore{String, valueType}(  bucketName; session=session, key_format=:string, 
                                          val_format=:data, empty=false, gzip=gzip, 
                                         debug=true) 
    
    @show kvStore
    GSDict( kvStore, bucketName, keyPrefix, session )
end

##################### properties ##############################
function get_info( self::GSDict )
    storage(:Object, :get, self.bucketName,
                   joinpath(dirname(strip(self.keyPrefix,'/')), "info"))
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
function Base.delete!( d::GSDict, key::String )
	#authorize( d.googleSession )
    # authorize( d.kvStore.session )
    delete!( d.kvStore, joinpath(d.keyPrefix, key) )
end

function Base.setindex!( d::GSDict, value::Any, key::String )
	#authorize( d.googleSession )
    # authorize( d.kvStore.session )
    d.kvStore[joinpath(d.keyPrefix, key)] = value 
end

function Base.getindex( d::GSDict, key::String)
	#authorize( d.googleSession )
    data = d.kvStore[joinpath(d.keyPrefix, key)] 
    return data
end

function Base.keys( d::GSDict )
    # keyList = keys( d.kvStore )
    # for i in eachindex(keyList)
    #     keyList[i] = joinpath(d.keyPrefix, keyList[i])
    # end
    # @show keyList
	authorize(d.googleSession)
    ds = storage(:Object, :list, d.bucketName; prefix=d.keyPrefix, fields="items(name)")
    ret = Vector{String}()
    for i in eachindex(ds)
        chunkFileName = replace(ds[i][:name], "$(rstrip(d.keyPrefix, '/'))/" => "" )
        push!(ret, chunkFileName)
    end
    return ret
end

function Base.haskey( d::GSDict, key::String )
    haskey(d.kvStore, joinpath(d.keyPrefix, key))
end

################### utility functions #################

function get_credential_filename()
    if isfile(expanduser("~/.google_credentials.json"))
        return expanduser("~/.google_credentials.json")
    elseif isfile(joinpath(dirname(@__FILE__), "../.google_credentials.json"))
        return joinpath(dirname(@__FILE__), "../.google_credentials.json")
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
    key = strip(key, '/')
    return String(bucketName), String(key)
end

end # end of module
