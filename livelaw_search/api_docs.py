search_api_parameters = [
    {
        "name": "search_term",
        "in": "body",
        "type": "string",
        "required": True,
        "example": "Indian Space Law",
    },
    {
        "name": "page",
        "in": "body",
        "type": "integer",
        "required": True,
        "example": "7",
    },
]
search_api_responses = {
    200: {
        "description": "Search successful",
        "examples": {
            "application/json": {
                "total_articles": 671,
                "search_result": [
                    {
                        "heading": "Insurance Company Not Liable If Motor Vehicle At Time Of Accident Was In Breach Of 'Purpose Of Use' As Per Policy: Telangana High Court",
                        "id": 197690,
                    },
                    {
                        "heading": "Parties Cannot Be Referred To Arbitration In Absence Of Privity Of Contract: Telangana High Court",
                        "id": 197658,
                    },
                    {
                        "heading": "Absence Of Rule Of Law Propels A Country Towards Inevitable Ruin, Duty Of Court To Take Strict View Of Non-Compliance Of Judicial Orders: Delhi HC",
                        "id": 197576,
                    },
                ],
                "current_page": 2,
            }
        },
    },
    400: {
        "description": "Error",
        "examples": {"application/json": {"message": "Please enter search query"}},
    },
    500: {
        "description": "Error",
        "examples": {"application/json": {"message": "Elasticsearch Connection error"}},
        }
}

insert_api_parameters = [
    {
        "name": "id",
        "in": "body",
        "type": "integer",
        "required": True,
        "example": "113",
    },
    {
        "name": "heading",
        "in": "body",
        "type": "string",
        "required": True,
        "example": "Absence Of Rule Of Law Propels A Country Towards Inevitable Ruin, Duty Of Court To Take Strict View Of Non-Compliance Of Judicial Orders: Delhi HC",
    },
    {
        "name": "keywords",
        "in": "body",
        "type": "string",
        "required": True,
        "example": "India",
    },
    {
        "name": "content",
        "in": "body",
        "type": "string",
        "required": True,
        "example": "Absence Of Rule Of Law Propels A Country Towards Inevitable Ruin, Duty Of Court To Take Strict View Of Non-Compliance Of Judicial Orders: Delhi HC",
    },
    {
        "name": "date",
        "in": "body",
        "type": "string",
        "required": True,
        "example": "2014-11-15T06:22:35",
    },
]
insert_api_responses = {
    201: {
        "description": "Insertion successful",
        "examples": {"application/json": {"message": "insertion task initiated"}},
    },
    500: {
        "description": "Error",
        "examples": {"application/json": {"message": "Elasticsearch Connection error"}},
        }
}
