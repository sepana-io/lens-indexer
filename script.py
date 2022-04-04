from datetime import datetime
import requests
from es_util import ElasticClient
from elasticsearch import helpers
import string
import json

es = ElasticClient()


def index_contents(contents, INDEX):
    try:
        actions = [
            {
                "_index": INDEX,
                "_source": content,
            } for content in contents
        ]
        helpers.bulk(es, actions)
        print(f"succesfully persist {len(contents)} docs")
    except Exception as e:
        print("Some hicups", e)


def get_metadata(url: str):
    try:
        import requests
        if "ipfs" in url:
            url = url.replace("ipfs://", "https://ipfs.io/ipfs/")
        url = url.replace(".json", "")
        res = requests.get(url)
        return res.json()
    except Exception as e:
        print("Error", url)


def map_post(post: dict):
    post["profile"] = post.pop("profileId", {})
    metadata = get_metadata(post.get("contentURI", ""))
    if metadata and "content" in metadata:
        post["metadata"] = metadata
    post["createdAt"] = datetime.fromtimestamp(
        int(post.get("timestamp", 100000)))
    if post.get("comments", []):
        post["comments"] = [map_post(c) for c in post.get("comments", [])]
    profile = get_profile(hex(int(post["profile"].get("id"))))
    if profile:
        post["profile"] = profile
    return post


def map_post_2(post: dict):
    post["has_error"] = True
    post["profile"] = post.pop("profileId", {})
    metadata = {
        "description": None,
        "content": post.get("contentURI").strip("data:,").strip(" "),
        "external_url": None,
        "image": None,
        "name": "",
        "attributes": [],
        "media": [],
        "animation_url": None
    }
    post["metadata"] = metadata
    post["createdAt"] = datetime.fromtimestamp(
        int(post.get("timestamp", 100000)))
    profile = get_profile(hex(int(post["profile"].get("id"))))
    if profile:
        post["profile"] = profile
    return post


def get_last_doc(index):
    q = {
        "size": 1,
        "sort": {"createdAt": "desc"},
        "query": {
            "match_all": {}
        }
    }
    return es.search(index=index, body=q)["hits"]["hits"][0]["_source"]


BASE_URL = "https://api.thegraph.com/subgraphs/name/anudit/lens-protocol"


def get_posts(last_timestamp="0"):
    query = f"""
        {{
        posts(first: 1000, orderBy:timestamp, where: {{ timestamp_gte: \"{last_timestamp}\" }}) {{
            id
            pubId
            profileId {{
            id
            handle
            owner
            pubCount
            imageURI
            createdOn
            creator
            followNFTURI
            followNFT
            }}
            contentURI
            timestamp
            collectModule
            comments {{
            id
            contentURI
            timestamp
            profileId
            pubId
            }}
        }}
        }}
    """
    data = requests.post(BASE_URL, json={"query": query}).json()
    return data.get("data", {}).get("posts", [])


def get_profiles(last_created_on="0"):
    query = f"""
        {{
        profiles(first: 1000, orderBy:createdOn, where: {{ createdOn_gte: \"{last_created_on}\" }}) {{
            id
            creator
            owner
            profileId
            pubCount
            followNFT
            handle
            imageURI
            followNFTURI
            createdOn
            followModule
            followModuleReturnData
            dispatcher
            }}
        }}
        }}
    """
    data = requests.post(BASE_URL, json={"query": query})
    return data.json()


def get_profile(profile_id):
    query = f"""
        query Profiles {{
          profiles(request: {{ profileIds: [\"{profile_id}\"], limit: 1 }}) {{
            items {{
              id
              name
              bio
              location
              website
              twitterUrl
              picture {{
                ... on NftImage {{
                  contractAddress
                  tokenId
                  uri
                  verified
                }}
                ... on MediaSet {{
                  original {{
                    url
                    mimeType
                  }}
                }}
                __typename
              }}
              handle
              coverPicture {{
                ... on NftImage {{
                  contractAddress
                  tokenId
                  uri
                  verified
                }}
                ... on MediaSet {{
                  original {{
                    url
                    mimeType
                  }}
                }}
                __typename
              }}
              ownedBy
              depatcher {{
                address
                canUseRelay
              }}
              stats {{
                totalFollowers
                totalFollowing
                totalPosts
                totalComments
                totalMirrors
                totalPublications
                totalCollects
              }}
              followModule {{
                ... on FeeFollowModuleSettings {{
                  type
                  amount {{
                    asset {{
                      symbol
                      name
                      decimals
                      address
                    }}
                    value
                  }}
                  recipient
                }}
                __typename
              }}
            }}
            pageInfo {{
              prev
              next
              totalCount
            }}
          }}
        }}
    """
    data = requests.post(
        "https://api-mumbai.lens.dev/playground", json={"query": query}).json()
    profiles = data.get("data", {}).get("profiles", {}).get("items", [])
    return profiles[0] if profiles else {}


def index_profiles():
    last_profile = {}
    while True:
        last_timestamp = last_profile.get("createdOn", "0")
        profiles = get_profiles(last_timestamp)
        if last_profile:
            for i in range(len(profiles)):
                if last_profile.get("id") == profiles[i].get("id"):
                    profiles = profiles[i+1:]
                    break
        if not profiles:
            break
        last_profile = profiles[-1]
        index_contents(profiles, "lens-new-profiles-data")


def index_posts():
    last_post = get_last_doc(index="lens-final-posts-data")
    while True:
        last_timestamp = last_post.get("timestamp", "0")
        posts = get_posts(last_timestamp)
        if posts:
            for i in range(len(posts)):
                if last_post.get("id") == posts[i].get("id"):
                    posts = posts[i+1:]
                    break
        if not posts:
            break
        last_post = posts[-1]
        new_posts = []
        print(f"mapping {len(posts)} posts")
        for post in posts:
            content = post.get("contentURI", "")
            if content.startswith("http") or ("ipfs" in content):
                new_posts.append(map_post(post))
            elif content.startswith("data:,"):
                new_posts.append(map_post_2(post))
        print(f"indexing {len(new_posts)} posts")
        index_contents(new_posts, "lens-final-posts-data")

def get_profiles_from_lens(q, offset = 0):
    query = f"""
        query Search {{
          search(request: {{
            query: \"{q}\",
            type: PROFILE,
            limit: 50,
            cursor: "{{\\"offset\\":{offset}}}"
          }}) {{
            ... on ProfileSearchResult {{
              __typename 
              items {{
                ... on Profile {{
                  ...ProfileFields
                }}
              }}
              pageInfo {{
                prev
                totalCount
                next
              }}
            }}
          }}
        }}

        fragment MediaFields on Media {{
          url
          mimeType
        }}

        fragment ProfileFields on Profile {{
          profileId: id,
          name
          bio
          location
          website
          twitterUrl
          handle
          picture {{
            ... on NftImage {{
              contractAddress
              tokenId
              uri
              verified
            }}
            ... on MediaSet {{
              original {{
                ...MediaFields
              }}
            }}
          }}
          coverPicture {{
            ... on NftImage {{
              contractAddress
              tokenId
              uri
              verified
            }}
            ... on MediaSet {{
              original {{
                ...MediaFields
              }}
            }}
          }}
          ownedBy
          depatcher {{
            address
          }}
          stats {{
            totalFollowers
            totalFollowing
            totalPosts
            totalComments
            totalMirrors
            totalPublications
            totalCollects
          }}
          followModule {{
            ... on FeeFollowModuleSettings {{
              type
              amount {{
                asset {{
                  name
                  symbol
                  decimals
                  address
                }}
                value
              }}
              recipient
            }}
          }}
        }}
    """
    try:
        data = requests.post("https://api-mumbai.lens.dev/playground", json={"query":query}).json()
        return data.get("data", {}).get("search", {})
    except Exception as e:
        print(e)
    return {}

def index_profiles():
    for c in string.ascii_lowercase:
        print("quering ::: ", c)
        offset = 0
        search_res = get_profiles_from_lens(c, offset)
        while search_res.get("items"):
            profiles = search_res.get("items")
            pageInfo = search_res.get("pageInfo")
            offset = json.loads(pageInfo.get("next", {})).get("offset", 100000)
            contents = []
            for profile in profiles:
                profile["id"] = profile.get("profileId", profile.get("id"))
                contents.append(profile)
            index_contents(contents, "lens-final-profiles-data")
            search_res = get_profiles_from_lens(c, offset)
        
def get_profile_ids():
    q = {
      "aggs": {
        "ids": {
          "terms": {
            "field": "id.keyword",
            "size": 10000
          }
        }
      },"size": 0
    }
    res = es.search(index="lens-final-profiles-data", body=q)
    return res.get("aggregations", {}).get("ids", {}).get("buckets", [])
        
        
def get_posts_by_profile_from_lens(profileId, next_cursor = ""):
    query = f"""
        query Publications {{
          publications(request: {{
            profileId: \"{profileId}\",
            publicationTypes: [POST, COMMENT, MIRROR],
            limit: 50,
            cursor: \"{next_cursor}\"
          }}) {{
            items {{
              __typename 
              ... on Post {{
                ...PostFields
              }}
              ... on Comment {{
                ...CommentFields
              }}
              ... on Mirror {{
                ...MirrorFields
              }}
            }}
            pageInfo {{
              prev
              next
              totalCount
            }}
          }}
        }}

        fragment MediaFields on Media {{
          url
          mimeType
        }}

        fragment ProfileFields on Profile {{
          id
          name
          bio
          location
          website
          twitterUrl
          handle
          picture {{
            ... on NftImage {{
              contractAddress
              tokenId
              uri
              verified
            }}
            ... on MediaSet {{
              original {{
                ...MediaFields
              }}
            }}
          }}
          coverPicture {{
            ... on NftImage {{
              contractAddress
              tokenId
              uri
              verified
            }}
            ... on MediaSet {{
              original {{
                ...MediaFields
              }}
            }}
          }}
          ownedBy
          depatcher {{
            address
          }}
          stats {{
            totalFollowers
            totalFollowing
            totalPosts
            totalComments
            totalMirrors
            totalPublications
            totalCollects
          }}
          followModule {{
            ... on FeeFollowModuleSettings {{
              type
              amount {{
                asset {{
                  name
                  symbol
                  decimals
                  address
                }}
                value
              }}
              recipient
            }}
          }}
        }}

        fragment PublicationStatsFields on PublicationStats {{ 
          totalAmountOfMirrors
          totalAmountOfCollects
          totalAmountOfComments
        }}

        fragment MetadataOutputFields on MetadataOutput {{
          name
          description
          content
          media {{
            original {{
              ...MediaFields
            }}
          }}
          attributes {{
            displayType
            traitType
            value
          }}
        }}

        fragment Erc20Fields on Erc20 {{
          name
          symbol
          decimals
          address
        }}

        fragment CollectModuleFields on CollectModule {{
          __typename
          ... on EmptyCollectModuleSettings {{
            type
          }}
          ... on FeeCollectModuleSettings {{
            type
            amount {{
              asset {{
                ...Erc20Fields
              }}
              value
            }}
            recipient
            referralFee
          }}
          ... on LimitedFeeCollectModuleSettings {{
            type
            collectLimit
            amount {{
              asset {{
                ...Erc20Fields
              }}
              value
            }}
            recipient
            referralFee
          }}
          ... on LimitedTimedFeeCollectModuleSettings {{
            type
            collectLimit
            amount {{
              asset {{
                ...Erc20Fields
              }}
              value
            }}
            recipient
            referralFee
            endTimestamp
          }}
          ... on RevertCollectModuleSettings {{
            type
          }}
          ... on TimedFeeCollectModuleSettings {{
            type
            amount {{
              asset {{
                ...Erc20Fields
              }}
              value
            }}
            recipient
            referralFee
            endTimestamp
          }}
        }}

        fragment PostFields on Post {{
          id
          profile {{
            ...ProfileFields
          }}
          stats {{
            ...PublicationStatsFields
          }}
          metadata {{
            ...MetadataOutputFields
          }}
          createdAt
          collectModule {{
            ...CollectModuleFields
          }}
          referenceModule {{
            ... on FollowOnlyReferenceModuleSettings {{
              type
            }}
          }}
          appId
        }}

        fragment MirrorBaseFields on Mirror {{
          id
          profile {{
            ...ProfileFields
          }}
          stats {{
            ...PublicationStatsFields
          }}
          metadata {{
            ...MetadataOutputFields
          }}
          createdAt
          collectModule {{
            ...CollectModuleFields
          }}
          referenceModule {{
            ... on FollowOnlyReferenceModuleSettings {{
              type
            }}
          }}
          appId
        }}

        fragment MirrorFields on Mirror {{
          ...MirrorBaseFields
          mirrorOf {{
           ... on Post {{
              ...PostFields          
           }}
           ... on Comment {{
              ...CommentFields          
           }}
          }}
        }}

        fragment CommentBaseFields on Comment {{
          id
          profile {{
            ...ProfileFields
          }}
          stats {{
            ...PublicationStatsFields
          }}
          metadata {{
            ...MetadataOutputFields
          }}
          createdAt
          collectModule {{
            ...CollectModuleFields
          }}
          referenceModule {{
            ... on FollowOnlyReferenceModuleSettings {{
              type
            }}
          }}
          appId
        }}

        fragment CommentFields on Comment {{
          ...CommentBaseFields
          mainPost {{
            ... on Post {{
              ...PostFields
            }}
            ... on Mirror {{
              ...MirrorBaseFields
              mirrorOf {{
                ... on Post {{
                   ...PostFields          
                }}
                ... on Comment {{
                   ...CommentMirrorOfFields        
                }}
              }}
            }}
          }}
        }}

        fragment CommentMirrorOfFields on Comment {{
          ...CommentBaseFields
          mainPost {{
            ... on Post {{
              ...PostFields
            }}
            ... on Mirror {{
               ...MirrorBaseFields
            }}
          }}
        }}
    """
    try:
        data = requests.post("https://api-mumbai.lens.dev/playground", json={"query":query}).json()
        return data.get("data", {}).get("publications", {})
    except Exception as e:
        print(e)
    return {}


def index_posts_from_lens():
    profiles_ids = get_profile_ids()
    for profile in profiles_ids:
        print("Searching ", profile)
        next_cursor = "{\\\"entityIdentifier\\\":\\\"\\\"}"
        search_res = get_posts_by_profile_from_lens(profile, next_cursor)
        while search_res.get("items"):
            posts = search_res.get("items")
            pageInfo = search_res.get("pageInfo")
            next_cursor = pageInfo.get("next", "")
            index_contents(posts, "lens-final-posts")
            search_res = get_posts_by_profile_from_lens(profile, next_cursor)