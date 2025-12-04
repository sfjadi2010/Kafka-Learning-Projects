export interface Record {
    id: number;
    row_number: number;
    filename: string;
    data: {
        [key: string]: string | number;
    };
    created_at: string;
}

export interface Stats {
    total_messages_consumed: number;
    last_consumed_at: string;
    status: string;
    current_consumer_active: boolean;
}

export interface ConsumerHealth {
    status: string;
    database: string;
}

export interface Topic {
    topic_name: string;
    table_name: string;
    record_count: number;
    created_at: string;
}
