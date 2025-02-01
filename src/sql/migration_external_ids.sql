alter table files
    add column external_id text not null default (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) ||
                                                  '-4' ||
                                                  substr(lower(hex(randomblob(2))), 2) || '-' ||
                                                  substr('89ab', abs(random()) % 4 + 1, 1) ||
                                                  substr(lower(hex(randomblob(2))), 2) || '-' ||
                                                  lower(hex(randomblob(6))));

alter table directory_entries
    add column external_id text not null default (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) ||
                                                  '-4' ||
                                                  substr(lower(hex(randomblob(2))), 2) || '-' ||
                                                  substr('89ab', abs(random()) % 4 + 1, 1) ||
                                                  substr(lower(hex(randomblob(2))), 2) || '-' ||
                                                  lower(hex(randomblob(6))));
