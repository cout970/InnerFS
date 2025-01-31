create table if not exists external_ids
(
    -- see https://stackoverflow.com/a/41649754
    external_id text not null default (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
                                       substr(lower(hex(randomblob(2))), 2) || '-' ||
                                       substr('89ab', abs(random()) % 4 + 1, 1) ||
                                       substr(lower(hex(randomblob(2))), 2) || '-' || lower(hex(randomblob(6)))),
    internal_id int  not null,
    type        int  not null, -- 0: file, 1: directory
    primary key (external_id),
    unique (type, internal_id)
);