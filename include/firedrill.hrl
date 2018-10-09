-record(fd_delay_req,
        {ref  :: reference()
        ,from :: pid()
        ,to   :: pid()
        ,type :: atom()
        ,data :: any()}).

-record(fd_delay_resp,
        {ref  :: reference()}).
