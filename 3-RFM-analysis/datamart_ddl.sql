CREATE TABLE analysis.dm_rfm_segments (
	user_id int not null primary key,
    recency int CHECK (recency between 1 and 5),
    frequency int CHECK (recency between 1 and 5),
    monetary_value int CHECK (recency between 1 and 5)
);
