export interface Record {
    id: number;
    [key: string]: string | number;
}

export interface Stats {
    total_records: number;
    consumer_status: string;
    last_updated: string;
}

export interface ConsumerHealth {
    status: string;
    database: string;
}
