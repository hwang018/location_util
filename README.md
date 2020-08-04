# location_util
A simple example using geohash to find co-location, given lat long location records.
The logic is first to hash lat long points into geohashed bins, with specified precision.
Then to associate time with hashed location value, to find co-location occurrences.
We can find the hours and locations that co-location happened, and also the places with the most co-locations.