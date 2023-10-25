# Sanctions screener

*NOTE* This is an incubating and experimental project.

Screen a user with `ofac` to see if they are sanctioned. 


Example: 

```sh
curl -X POST http://127.0.0.1:5000/screen_entity \
     -H "Content-Type: application/json" \
     -d '{
           "query": {
             "min_score": 0.9,
             "name": "Peter Griffin",


             "dob": "1935-01-01T00:00:00.00Z",
             "dob_months_range": 12
           }
         }'
```

will yield results: 

```json
{
  "hits": [
    {
      "name": "GRIFFIN, Peter"
    }
  ],
  "total_hits": 1
}
```

# Installing and running

python -r requirements.txt
python server.py

There is also a `go-version` that is being experimented with as an alternative (but doesn't have the same fuzzy search libraries yet as Python)

# Read about OFAC

https://sanctionssearch.ofac.treas.gov/
