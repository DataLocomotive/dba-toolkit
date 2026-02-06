DECLARE
	@DeleteEndDate	DATE		= '2021-01-01',
	@BatchSize		SMALLINT	= 5000,
	@Delay			CHAR(8)		= '00:00:01',
	@RowCount		SMALLINT;

	SET NOCOUNT ON;

	-- Staging table for rows to be deleted
	DROP TABLE IF EXISTS #job_history_delete;
	CREATE TABLE #job_history_delete (job_history_id INT NOT NULL);

	-- Get the primary keys for all rows we want to delete
	INSERT INTO #job_history_delete (job_history_id)
	SELECT job_history_id
	FROM [dbo].[job_history]
	WHERE dtCreated <= @DeleteEndDate;

	-- Table for each batch to delete
	DROP TABLE IF EXISTS #CurrentBatch;
	CREATE TABLE #CurrentBatch (job_history_id INT NOT NULL);

	SET @RowCount = 1;
	WHILE @RowCount > 0
	BEGIN
		-- Get a new batch
		INSERT INTO #CurrentBatch (job_history_id)
		SELECT TOP (@BatchSize) job_history_id
		FROM #job_history_delete;

		SET @RowCount = @@ROWCOUNT;

		-- Delete the current batch
		DELETE [dbo].[job_history]
		FROM #CurrentBatch
		INNER JOIN [dbo].[job_history] jh ON jh.job_history_id = #CurrentBatch.job_history_id;

		-- Delete the batch from the staging table
		DELETE jhd FROM #job_history_delete jhd
		INNER JOIN #CurrentBatch cb ON jhd.job_history_id = cb.job_history_id;

		-- Clear out the batch table
		TRUNCATE TABLE #CurrentBatch;

		-- Wait between batches to reduce impact
		WAITFOR DELAY @Delay;
	END;