For this ticket we can do it and returns all data in one response for Latest News Section, But:

We need to create a new endpoint that will returns all data that we need to display in latest news section (from Editor, NewsFeed and Latest news endpoints).
why we need new endpoint? because we need to keep each one separated for other pages or different use

For the response that will returns from it we need to build new struct that accept all data from all sources this mean we need to match between the three sources (we need to discuss it with FE to see what the fields they need from all sources).

Or we can build struct that accept all articles type from all sources.

for this endpoint we need to build the business logic again to accept the changes:

we can take this logic:
    - we need to send (editor, newsfeeds) as first selected in query filter and this will be added by default. 
    - We can add others pills after the first one. 
    - the response will be divided between all pills in query filter.
    - What I mean let's assume you select (editor (by default) and bitcoin) the response will be the top 6 articles from editor and top 6 from bitcoin and this will be changed by the numbers of pills in Query filter.




If we need to change the response it will be only for this page this mean we can’t use this endpoint for any other pages because the logic is different now..

There are differences in data that we returns from each source from these three sources, So we need to build a struct that accept the articles data from all sources with all fields that can be provided from each one if them.

We must check with FE what the data they need from all sources to know what the data that missing from the sources to be add it.

This will cause repeated code because this code has been written before for each endpoint (same process for each one will be calling in one function).

The change will be also for latest news articles if we need to use it 