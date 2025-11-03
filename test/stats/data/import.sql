
\set localpath `pwd`'/users.csv'
COPY users (
    Id,
    Reputation,
    CreationDate,
    DisplayName,
    LastAccessDate,
    WebsiteUrl,
    Location,
    AboutMe,
    Views,
    UpVotes,
    DownVotes,
    AccountId,
    Age,
    ProfileImageUrl)
FROM :'localpath'
DELIMITER ',' NULL 'NULL' CSV HEADER;

\set localpath `pwd`'/badges.csv'
COPY badges (Id, UserId, Name, Date)
FROM :'localpath'
DELIMITER ',' NULL 'NULL' CSV HEADER;

\set localpath `pwd`'/posts.csv'
COPY posts (
    Id,
    PostTypeId,
    AcceptedAnswerId,
    CreationDate,
    Score,
    ViewCount,
    Body,
    OwnerUserId,
    LasActivityDate,
    Title,
    Tags,
    AnswerCount,
    CommentCount,
    FavoriteCount,
    LastEditorUserId,
    LastEditDate,
    CommunityOwnedDate,
    ParentId,
    ClosedDate,
    OwnerDisplayName,
    LastEditorDisplayName)
FROM :'localpath'
DELIMITER ',' NULL 'NULL' CSV HEADER;

\set localpath `pwd`'/tags.csv'
COPY tags (Id, TagName, Count, ExcerptPostId, WikiPostId)
FROM :'localpath'
DELIMITER ',' NULL 'NULL' CSV HEADER;

\set localpath `pwd`'/postLinks.csv'
COPY postLinks (Id, CreationDate, PostId, RelatedPostId, LinkTypeId)
FROM :'localpath'
DELIMITER ',' NULL 'NULL' CSV HEADER;

\set localpath `pwd`'/postHistory.csv'
COPY postHistory (
    Id,
    PostHistoryTypeId,
    PostId,
    RevisionGUID,
    CreationDate,
    UserId,
    Text,
    Comment,
    UserDisplayName)
FROM :'localpath'
DELIMITER ',' NULL 'NULL' CSV HEADER;

\set localpath `pwd`'/comments.csv'
COPY comments (
    Id,
    PostId,
    Score,
    Text,
    CreationDate,
    UserId,
    UserDisplayName)
FROM :'localpath'
DELIMITER ',' NULL 'NULL' CSV HEADER;


\set localpath `pwd`'/votes.csv'
COPY votes (Id, PostId, VoteTypeId, CreationDate, UserId, BountyAmount)
FROM :'localpath'
DELIMITER ',' NULL 'NULL' CSV HEADER;
