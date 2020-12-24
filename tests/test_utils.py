#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime

import pytest

from sqlcache import utils


class TestCache:
    def test_normalize_query(self):
        query1 = "select top 3 * from receipts"
        query2 = "SELECT top 3 * FROM receipts"

        assert utils.normalize_query(query1) == utils.normalize_query(query2)

        query1 = """
        SELECT TIN, COUNT(SdcReceiptSignature)  AS ReceiptCount, SUM(TaxableAmount2) as TaxbleAmount
        FROM Receipts
        WHERE SdcDateTime BETWEEN '20180601' AND '20180609' AND ReceiptType='N' GROUP BY TIN
        """

        query2 = """
        SELECT
            TIN,
            COUNT(SdcReceiptSignature)  AS ReceiptCount,
            SUM(TaxableAmount2) as TaxbleAmount
        FROM
            Receipts
        WHERE
            SdcDateTime BETWEEN '20180601' AND '20180609'
        AND
            ReceiptType='N'
        GROUP BY
            TIN
        """

        assert utils.normalize_query(query1) == utils.normalize_query(query2)

        query1 = "select top 3 * from receipts"
        query2 = "SELECT top 3 * FROM Receipts"

        assert utils.normalize_query(query1) != utils.normalize_query(
            query2
        ), "Identifier names (table names and columns) should not be normalized"

    def test_normalize_query_with_comments(self):
        query1 = "select top 3 * from receipts -- I have a comment"
        query2 = "SELECT top 3 * FROM receipts"

        assert utils.normalize_query(query1) == utils.normalize_query(query2)

        query1 = """
        SELECT TIN, COUNT(SdcReceiptSignature)  AS ReceiptCount, SUM(TaxableAmount2) as TaxbleAmount
        FROM Receipts
        WHERE SdcDateTime BETWEEN '20180601' AND '20180609' AND ReceiptType='N' GROUP BY TIN
        """

        query2 = """
        SELECT
            -- I have a comment
            TIN,
            COUNT(SdcReceiptSignature)  AS ReceiptCount,
            SUM(TaxableAmount2) as TaxbleAmount
        FROM
            Receipts
        WHERE
            SdcDateTime BETWEEN '20180601' AND '20180609'
        AND
            ReceiptType='N'
        GROUP BY
            TIN
        """

        assert utils.normalize_query(query1) == utils.normalize_query(query2)

    def test_normalize_query_with_tabs(self):
        query1 = """
        select top 1
        	tin,
        	Journal
        from
        	Receipts
        """
        query2 = """
        select top 1
            tin,
            Journal
        from
            Receipts
        """

        assert utils.normalize_query(query1) == utils.normalize_query(query2)

    def test_normalize_query_with_big_query(self):
        ids = tuple(range(40000))
        query = f"select * from table where id in ({ids})"
        assert utils.normalize_query(query) == query
