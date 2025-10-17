import streamlit as st
import pandas as pd
from datetime import datetime, date, time
import io

# Performance optimization
@st.cache_data
def load_csv_file(file_content, parse_dates_cols):
    """Cache CSV loading for better performance"""
    return pd.read_csv(io.BytesIO(file_content), parse_dates=parse_dates_cols)

@st.cache_data
def load_excel_file(file_content, parse_dates_cols):
    """Cache Excel loading for better performance"""
    return pd.read_excel(io.BytesIO(file_content), parse_dates=parse_dates_cols)

st.set_page_config(page_title="Zen vs BridgerPay vs Coins Buy Comparator", layout="wide")
st.title("ZEN vs BridgerPay vs Coins Buy Comparator")

# Create three tabs: ZEN, BridgerPay, and Coins Buy
tab_zen, tab_bp, tab_coins = st.tabs(["ZEN", "BridgerPay", "Coins Buy"])

# Futures filtering function - checks if "Futures" is in the Plan Type name
def is_futures_plan(plan_type):
    """Check if a plan type contains 'Futures' (case-insensitive)"""
    if pd.isna(plan_type):
        return False
    return "futures" in str(plan_type).lower()

# --- ZEN Tab ---
with tab_zen:
    st.header("ZEN vs Order-list")
    zen_file = st.file_uploader("Upload ZEN Settlement File", key="zen_file", type=["csv", "xlsx"])
    order_file_zen = st.file_uploader("Upload Order-list File for ZEN", key="order_file_zen", type=["csv", "xlsx"])

    if zen_file and order_file_zen:
        # Load ZEN with caching
        with st.spinner("Loading ZEN file..."):
            if zen_file.name.lower().endswith('.csv'):
                df_zen = load_csv_file(zen_file.read(), ["accepted_at"])
            else:
                df_zen = load_excel_file(zen_file.read(), ["accepted_at"])
            df_zen["accepted_at"] = df_zen["accepted_at"].dt.tz_localize(None)

        # Validate gateway columns
        if not (df_zen.get("Gateway", pd.Series()).eq("Zen Pay").all()):
            st.error("ZEN file must have Gateway='Zen Pay'")
            st.stop()

        # Load Order-list
        if order_file_zen.name.lower().endswith('.csv'):
            df_ord = pd.read_csv(order_file_zen, parse_dates=["Updated At"])
        else:
            df_ord = pd.read_excel(order_file_zen, parse_dates=["Updated At"])
        
        if not all(g == "Zen Pay" for g in df_ord.get("Gateway", pd.Series()).unique()):
            st.error("Order-list must have Gateway='Zen Pay'")
            st.stop()
        df_ord = df_ord[df_ord.get("Gateway", "") == "Zen Pay"].copy()

        # Date range selection based on ZEN data
        min_date = df_zen['accepted_at'].dt.date.min()
        max_date = df_zen['accepted_at'].dt.date.max()
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start date", value=min_date)
        with col2:
            end_date = st.date_input("End date", value=max_date)

        # Define head/tail windows
        start_zen = datetime.combine(start_date, time(18, 0, 0))
        end_zen = datetime.combine(end_date, time(17, 59, 59))
        start_ord = datetime.combine(start_date, time(21, 0, 0))
        end_ord = datetime.combine(end_date, time(20, 59, 59))

        # Filter ZEN
        df_zen_filt = df_zen[
            (df_zen["accepted_at"] >= start_zen) &
            (df_zen["accepted_at"] <= end_zen) &
            (df_zen["payment_channel"].str.lower() != "card") &
            (df_zen["transaction_type"].str.lower() == "purchase")
        ].copy()

        # Filter non-USD and duplicates
        mask_currency_zen = df_zen_filt["transaction_currency"].str.upper() != "USD"
        if mask_currency_zen.any():
            st.warning(f"Excluded {mask_currency_zen.sum()} non-USD transactions")
            df_zen_filt = df_zen_filt.loc[~mask_currency_zen]

        initial_zen_count = len(df_zen_filt)
        duplicates_zen = df_zen_filt.duplicated(subset=["merchant_transaction_id"], keep=False)
        if duplicates_zen.any():
            st.warning(f"Removed {duplicates_zen.sum()} duplicates from {initial_zen_count} ZEN transactions")
            df_zen_filt = df_zen_filt.drop_duplicates(subset=["merchant_transaction_id"], keep="first")
        st.info(f"ZEN PSP: {len(df_zen_filt)} clean transactions")

        # Clean Order List
        initial_ord_count = len(df_ord)
        duplicates_ord = df_ord.duplicated(subset=["Transaction ID"], keep=False)
        if duplicates_ord.any():
            st.warning(f"Removed {duplicates_ord.sum()} duplicates from Order List")
            df_ord = df_ord.drop_duplicates(subset=["Transaction ID"], keep="first")
        st.info(f"Order List: {len(df_ord)} clean entries")

        # Filter Order-list and select needed columns
        df_ord_filt = df_ord[
            (df_ord["Updated At"] >= start_ord) &
            (df_ord["Updated At"] <= end_ord)
        ][["Transaction ID", "Plan Type", "Grand Total"]].copy()

        # Merge on transaction ID
        df_merged = df_zen_filt.merge(
            df_ord_filt,
            left_on="merchant_transaction_id",
            right_on="Transaction ID",
            how="inner"
        )

        # Check amount mismatches
        mask_amt = df_merged["transaction_amount"] != df_merged["Grand Total"]
        if mask_amt.any():
            st.warning(f"Found {mask_amt.sum()} amount mismatches in ZEN:")
            st.dataframe(df_merged.loc[mask_amt, [
                "merchant_transaction_id", "transaction_amount", "Grand Total", "transaction_currency"
            ]])
        else:
            st.success("No amount mismatches")
        
        # Handle unmatched PSP entries (add to CFD)
        unmatched_zen_psp = df_zen_filt[~df_zen_filt["merchant_transaction_id"].isin(df_merged["merchant_transaction_id"])]
        if len(unmatched_zen_psp) > 0:
            st.warning(f"Found {len(unmatched_zen_psp)} unmatched ZEN PSP entries - adding to CFD:")
            st.dataframe(unmatched_zen_psp[["merchant_transaction_id", "accepted_at", "transaction_amount", "transaction_currency"]])
            # Add unmatched entries to CFD
            unmatched_zen_psp = unmatched_zen_psp.copy()
            unmatched_zen_psp["Plan Type"] = "CFD (Unmatched PSP)"
            unmatched_zen_psp["Grand Total"] = unmatched_zen_psp["transaction_amount"]
            df_merged = pd.concat([df_merged, unmatched_zen_psp], ignore_index=True)

        st.info(f"Final total: {len(df_merged)} transactions included in export")

        # Split into Futures vs CFD
        df_futures = df_merged[df_merged["Plan Type"].apply(is_futures_plan)].copy()
        df_cfd = df_merged[~df_merged["Plan Type"].apply(is_futures_plan)].copy()

        # Sort by datetime before export
        df_futures = df_futures.sort_values("accepted_at")
        df_cfd = df_cfd.sort_values("accepted_at")

        # Revenue summary (GMT+6 shift on accepted_at)
        df_futures["Date"] = (df_futures["accepted_at"] + pd.Timedelta(hours=6)).dt.date
        df_cfd["Date"] = (df_cfd["accepted_at"] + pd.Timedelta(hours=6)).dt.date
        df_futures["Category"] = "Futures"
        df_cfd["Category"] = "CFD"
        df_summary = pd.concat([
            df_cfd[["Date", "Category", "transaction_amount"]],
            df_futures[["Date", "Category", "transaction_amount"]]
        ])
        df_summary = df_summary.groupby(["Date", "Category"], as_index=False).agg(Revenue=("transaction_amount", "sum")).sort_values("Date")

        st.subheader("Datewise Revenue Summary (GMT+6)")
        st.dataframe(df_summary)

        # Excel output for ZEN
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            cols = df_zen.columns.tolist()
            df_cfd[cols].to_excel(writer, sheet_name='CFD', index=False)
            df_futures[cols].to_excel(writer, sheet_name='Futures', index=False)
            df_summary.to_excel(writer, sheet_name='Revenue Summary', index=False)
            for sheet_name, df_out in [('CFD', df_cfd), ('Futures', df_futures), ('Revenue Summary', df_summary)]:
                ws = writer.sheets[sheet_name]
                for idx, col in enumerate(df_out.columns):
                    max_len = max(df_out[col].astype(str).map(len).max(), len(col)) + 2
                    ws.set_column(idx, idx, max_len)

        st.download_button(
            label="Download ZEN Comparison Report",
            data=output.getvalue(),
            file_name="zen_order_comparison.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# --- BridgerPay Tab ---
with tab_bp:
    st.header("BridgerPay vs Order-list")
    bp_file = st.file_uploader("Upload BridgerPay File", key="bp_file", type=["csv", "xlsx"])
    order_file_bp = st.file_uploader("Upload Order-list File for BridgerPay", key="order_file_bp", type=["csv", "xlsx"])

    if bp_file and order_file_bp:
        # Load BridgerPay
        if bp_file.name.lower().endswith('.csv'):
            df_bp = pd.read_csv(bp_file, parse_dates=["processing_date"])
        else:
            df_bp = pd.read_excel(bp_file, parse_dates=["processing_date"])
        df_bp["processing_date"] = df_bp["processing_date"].dt.tz_localize(None)

        # Validate and filter BridgerPay
        if not (df_bp.get("Gateway", pd.Series()).eq("Bridger Pay").all()):
            st.error("BridgerPay file must have Gateway='Bridger Pay'")
            st.stop()

        df_bp = df_bp[
            (df_bp.get("status", "").str.lower() == "approved") &
            (df_bp.get("type", "").str.lower() == "payment") &
            (df_bp.get("currency", "").str.upper() == "USD")
        ]

        # Remove duplicates
        initial_count = len(df_bp)
        duplicates_bp = df_bp.duplicated(subset=["merchantOrderId"], keep=False)
        if duplicates_bp.any():
            st.warning(f"Removed {duplicates_bp.sum()} duplicates from {initial_count} BridgerPay transactions")
            df_bp = df_bp.drop_duplicates(subset=["merchantOrderId"], keep="first")
        st.info(f"BridgerPay PSP: {len(df_bp)} clean transactions")

        # Date range selection based on BridgerPay processing_date
        min_date_bp = df_bp['processing_date'].dt.date.min()
        max_date_bp = df_bp['processing_date'].dt.date.max()
        col1_bp, col2_bp = st.columns(2)
        with col1_bp:
            start_date_bp = st.date_input("Start date", value=min_date_bp, key="bp_start_date")
        with col2_bp:
            end_date_bp = st.date_input("End date", value=max_date_bp, key="bp_end_date")
        # Filter BridgerPay data for selected window (00:00 to 23:59:59)
        start_proc = datetime.combine(start_date_bp, time(0, 0, 0))
        end_proc   = datetime.combine(end_date_bp,   time(23, 59, 59))
        df_bp = df_bp[(df_bp['processing_date'] >= start_proc) & (df_bp['processing_date'] <= end_proc)].copy()

        # Sort oldest→newest
        df_bp = df_bp.sort_values("processing_date")

        # Process Order List
        if order_file_bp.name.lower().endswith('.csv'):
            df_ord2 = pd.read_csv(order_file_bp, parse_dates=["Updated At"])
        else:
            df_ord2 = pd.read_excel(order_file_bp, parse_dates=["Updated At"])
        
        if not all(g == "Bridger Pay" for g in df_ord2.get("Gateway", pd.Series()).unique()):
            st.error("Order-list must have Gateway='Bridger Pay'")
            st.stop()
        df_ord2 = df_ord2[df_ord2.get("Gateway", "") == "Bridger Pay"].copy()
        
        # Remove duplicates
        duplicates_ord = df_ord2.duplicated(subset=["Transaction ID"], keep=False)
        if duplicates_ord.any():
            st.warning(f"Removed {duplicates_ord.sum()} duplicates from Order List")
            df_ord2 = df_ord2.drop_duplicates(subset=["Transaction ID"], keep="first")
        st.info(f"BridgerPay Order List: {len(df_ord2)} clean entries")
        
        # Filter Order-list for the same window
        df_ord2 = df_ord2[(df_ord2["Updated At"] >= start_proc) & (df_ord2["Updated At"] <= end_proc)].copy()

        # Merge and amount reconciliation for BP
        df_ord2_sel = df_ord2[["Transaction ID", "Plan Type", "Grand Total"]].copy()
        df_merged2 = df_bp.merge(
            df_ord2_sel,
            left_on="merchantOrderId",
            right_on="Transaction ID",
            how="inner"
        )
        mask_amt2 = df_merged2["amount"] != df_merged2["Grand Total"]
        if mask_amt2.any():
            st.warning(f"Found {mask_amt2.sum()} amount mismatches in BridgerPay:")
            st.dataframe(df_merged2.loc[mask_amt2, [
                "merchantOrderId", "amount", "Grand Total", "currency"
            ]])
        else:
            st.success("No amount mismatches")
        
        # Handle unmatched PSP entries (add to CFD)
        unmatched_bp_psp = df_bp[~df_bp["merchantOrderId"].isin(df_merged2["merchantOrderId"])]
        if len(unmatched_bp_psp) > 0:
            st.warning(f"Found {len(unmatched_bp_psp)} unmatched BridgerPay PSP entries - adding to CFD:")
            st.dataframe(unmatched_bp_psp[["merchantOrderId", "processing_date", "amount", "currency"]])
            # Add unmatched entries to CFD
            unmatched_bp_psp = unmatched_bp_psp.copy()
            unmatched_bp_psp["Plan Type"] = "CFD (Unmatched PSP)"
            unmatched_bp_psp["Grand Total"] = unmatched_bp_psp["amount"]
            df_merged2 = pd.concat([df_merged2, unmatched_bp_psp], ignore_index=True)

        st.info(f"Final total: {len(df_merged2)} transactions included in export")

        # Split into Futures vs CFD
        df_futures2 = df_merged2[df_merged2["Plan Type"].apply(is_futures_plan)].copy()
        df_cfd2 = df_merged2[~df_merged2["Plan Type"].apply(is_futures_plan)].copy()

        # Sort by datetime before export
        df_futures2 = df_futures2.sort_values("processing_date")
        df_cfd2 = df_cfd2.sort_values("processing_date")

        # Revenue summary (GMT+6 shift on processing_date)
        df_futures2["Date"] = (df_futures2["processing_date"] + pd.Timedelta(hours=6)).dt.date
        df_cfd2["Date"] = (df_cfd2["processing_date"] + pd.Timedelta(hours=6)).dt.date
        df_futures2["Category"] = "Futures"
        df_cfd2["Category"] = "CFD"
        df_summary2 = pd.concat([
            df_cfd2[["Date", "Category", "amount"]],
            df_futures2[["Date", "Category", "amount"]]
        ])
        df_summary2 = df_summary2.groupby(["Date", "Category"], as_index=False).agg(Revenue=("amount", "sum")).sort_values("Date")

        st.subheader("Datewise Revenue Summary (GMT+6)")
        st.dataframe(df_summary2)

        # Excel output for BP
        output2 = io.BytesIO()
        with pd.ExcelWriter(output2, engine="xlsxwriter") as writer:
            cols_bp = df_bp.columns.tolist()
            df_cfd2[cols_bp].to_excel(writer, sheet_name='CFD', index=False)
            df_futures2[cols_bp].to_excel(writer, sheet_name='Futures', index=False)
            df_summary2.to_excel(writer, sheet_name='Revenue Summary', index=False)
            for sheet_name, df_out in [('CFD', df_cfd2), ('Futures', df_futures2), ('Revenue Summary', df_summary2)]:
                ws = writer.sheets[sheet_name]
                for idx, col in enumerate(df_out.columns):
                    max_len = max(df_out[col].astype(str).map(len).max(), len(col)) + 2
                    ws.set_column(idx, idx, max_len)

        st.download_button(
            label="Download BridgerPay Comparison Report",
            data=output2.getvalue(),
            file_name="bridgerpay_order_comparison.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("Please upload both the BridgerPay file and the Order-list file for BridgerPay.")

# --- Coins Buy Tab ---
with tab_coins:
    st.header("Coins Buy vs Order-list")
    coins_file = st.file_uploader("Upload Coins Buy File", key="coins_file", type=["csv", "xlsx"])
    order_files_coins = st.file_uploader("Upload Order-list Files for Coins Buy (Multiple files allowed)", key="order_files_coins", type=["csv", "xlsx"], accept_multiple_files=True)

    if coins_file and order_files_coins:
        # Load Coins Buy
        if coins_file.name.lower().endswith('.csv'):
            df_coins = pd.read_csv(coins_file, parse_dates=["Created"])
        else:
            df_coins = pd.read_excel(coins_file, parse_dates=["Created"])
        df_coins["Created"] = df_coins["Created"].dt.tz_localize(None)

        # Calculate actual amount as Amount * Rate
        if "Amount" not in df_coins.columns or "Rate" not in df_coins.columns:
            st.error("Coins Buy file must contain 'Amount' and 'Rate' columns for calculation.")
            st.stop()
        df_coins["calculated_amount"] = df_coins["Amount"] * df_coins["Rate"]
        
        # Filter and check duplicates
        mask_high_amount = df_coins["calculated_amount"] > 2500
        if mask_high_amount.any():
            st.warning(f"Excluded {mask_high_amount.sum()} transactions > 2500:")
            excluded_transactions = df_coins[mask_high_amount][["Tracking ID", "Created", "Amount", "Rate", "calculated_amount"]]
            st.dataframe(excluded_transactions)
            df_coins = df_coins[~mask_high_amount].copy()

        duplicates_coins = df_coins.duplicated(subset=["Tracking ID"], keep=False)
        if duplicates_coins.any():
            st.info(f"Keeping {duplicates_coins.sum()} PSP duplicates for revenue:")
            duplicate_coins_data = df_coins[duplicates_coins].sort_values("Tracking ID")
            st.dataframe(duplicate_coins_data[["Tracking ID", "Created", "Amount", "Rate", "calculated_amount"]])
        st.info(f"Coins Buy PSP: {len(df_coins)} total transactions")

        # Date range selection based on Coins Buy Created date
        min_date_coins = df_coins['Created'].dt.date.min()
        max_date_coins = df_coins['Created'].dt.date.max()
        col1_coins, col2_coins = st.columns(2)
        with col1_coins:
            start_date_coins = st.date_input("Start date", value=min_date_coins, key="coins_start_date")
        with col2_coins:
            end_date_coins = st.date_input("End date", value=max_date_coins, key="coins_end_date")
        
        # Filter Coins Buy data for selected window (00:00 to 23:59:59)
        start_created = datetime.combine(start_date_coins, time(0, 0, 0))
        end_created = datetime.combine(end_date_coins, time(23, 59, 59))
        df_coins = df_coins[(df_coins['Created'] >= start_created) & (df_coins['Created'] <= end_created)].copy()

        # Sort oldest→newest
        df_coins = df_coins.sort_values("Created")

        # Load and merge multiple Order-list files for Coins Buy
        st.subheader("Step 3: Process Coins Buy Order List Files")
        st.info(f"Processing {len(order_files_coins)} Order List file(s)")
        
        # Load all Order List files
        order_list_dfs = []
        for i, order_file in enumerate(order_files_coins):
            if order_file.name.lower().endswith('.csv'):
                df_temp = pd.read_csv(order_file, parse_dates=["Updated At"])
            else:
                df_temp = pd.read_excel(order_file, parse_dates=["Updated At"])
            st.info(f"File {i+1} ({order_file.name}): {len(df_temp)} entries")
            order_list_dfs.append(df_temp)
        
        # Merge all Order List files
        df_ord3 = pd.concat(order_list_dfs, ignore_index=True)
        st.info(f"Combined Order List: {len(df_ord3)} total entries before cleaning")
        
        # Check if Tracking ID exists in both files
        if "Tracking ID" not in df_coins.columns:
            st.error("Coins Buy file must contain 'Tracking ID' column for matching.")
            st.stop()
        if "Tracking ID" not in df_ord3.columns:
            st.error("Order-list file must contain 'Tracking ID' column for matching.")
            st.stop()

        # Clean merged Order List: Remove None entries, duplicates, and sort by datetime
        initial_ord3_count = len(df_ord3)
        
        # Remove None/blank Tracking ID entries
        mask_none_tracking = df_ord3["Tracking ID"].isna() | (df_ord3["Tracking ID"].astype(str).str.strip().isin(["", "None", "none"]))
        if mask_none_tracking.any():
            st.warning(f"Removed {mask_none_tracking.sum()} 'None' entries from merged Order List")
            df_ord3 = df_ord3[~mask_none_tracking].copy()
        
        # Remove duplicates (keep first occurrence)
        duplicates_ord3 = df_ord3.duplicated(subset=["Tracking ID"], keep=False)
        if duplicates_ord3.any():
            st.warning(f"Removed {duplicates_ord3.sum()} duplicates from merged Order List")
            df_ord3 = df_ord3.drop_duplicates(subset=["Tracking ID"], keep="first")
        
        # Sort by Updated At datetime
        df_ord3 = df_ord3.sort_values("Updated At")
        
        st.success(f"Final merged Order List: {len(df_ord3)} clean entries (sorted by datetime)")

        # Filter Order-list for the same window
        df_ord3 = df_ord3[(df_ord3["Updated At"] >= start_created) & (df_ord3["Updated At"] <= end_created)].copy()

        # Handle blank Tracking ID and match
        mask_blank_tracking = df_coins["Tracking ID"].isna() | (df_coins["Tracking ID"].astype(str).str.strip() == "")
        df_blank_tracking = df_coins[mask_blank_tracking].copy()
        df_coins_with_tracking = df_coins[~mask_blank_tracking].copy()
        
        if len(df_blank_tracking) > 0:
            st.warning(f"Assigned {len(df_blank_tracking)} blank Tracking ID entries to CFD")
            df_blank_tracking["Plan Type"] = "CFD (Blank Tracking ID)"
            df_blank_tracking["Grand Total"] = df_blank_tracking["calculated_amount"]

        # Match with Order List
        df_ord3_sel = df_ord3[["Tracking ID", "Plan Type", "Grand Total"]].copy()
        df_merged3 = df_coins_with_tracking.merge(df_ord3_sel, on="Tracking ID", how="inner")
        
        if len(df_blank_tracking) > 0:
            df_merged3 = pd.concat([df_merged3, df_blank_tracking], ignore_index=True)
        
        initial_matched_count = len(df_merged3)
        st.success(f"Initial matches: {initial_matched_count - len(df_blank_tracking)} with Order List + {len(df_blank_tracking)} CFD (blank)")
        
        # Handle unmatched PSP entries (add to CFD)
        unmatched_coins_psp = df_coins_with_tracking[~df_coins_with_tracking["Tracking ID"].isin(df_ord3["Tracking ID"])]
        if len(unmatched_coins_psp) > 0:
            st.warning(f"Found {len(unmatched_coins_psp)} unmatched Coins Buy PSP entries - adding to CFD:")
            st.dataframe(unmatched_coins_psp[["Tracking ID", "Created", "calculated_amount"]])
            # Add unmatched entries to CFD
            unmatched_coins_psp = unmatched_coins_psp.copy()
            unmatched_coins_psp["Plan Type"] = "CFD (Unmatched PSP)"
            unmatched_coins_psp["Grand Total"] = unmatched_coins_psp["calculated_amount"]
            df_merged3 = pd.concat([df_merged3, unmatched_coins_psp], ignore_index=True)

        st.info(f"Final total: {len(df_merged3)} transactions included in export")

        # Sort all entries by Created datetime
        df_merged3 = df_merged3.sort_values("Created")

        # Amount reconciliation (no display needed for Coins Buy)

        # Split into Futures vs CFD
        df_futures3 = df_merged3[df_merged3["Plan Type"].apply(is_futures_plan)].copy()
        df_cfd3 = df_merged3[~df_merged3["Plan Type"].apply(is_futures_plan)].copy()

        # Revenue summary (GMT+6 shift on Created)
        df_futures3["Date"] = (df_futures3["Created"] + pd.Timedelta(hours=6)).dt.date
        df_cfd3["Date"] = (df_cfd3["Created"] + pd.Timedelta(hours=6)).dt.date
        df_futures3["Category"] = "Futures"
        df_cfd3["Category"] = "CFD"
        df_summary3 = pd.concat([
            df_cfd3[["Date", "Category", "calculated_amount"]],
            df_futures3[["Date", "Category", "calculated_amount"]]
        ])
        df_summary3 = df_summary3.groupby(["Date", "Category"], as_index=False).agg(Revenue=("calculated_amount", "sum"))

        st.subheader("Datewise Revenue Summary (GMT+6)")
        st.dataframe(df_summary3)

        # Excel output for Coins Buy
        output3 = io.BytesIO()
        with pd.ExcelWriter(output3, engine="xlsxwriter") as writer:
            cols_coins = df_coins.columns.tolist()
            df_cfd3[cols_coins].to_excel(writer, sheet_name='CFD', index=False)
            df_futures3[cols_coins].to_excel(writer, sheet_name='Futures', index=False)
            df_summary3.to_excel(writer, sheet_name='Revenue Summary', index=False)
            for sheet_name, df_out in [('CFD', df_cfd3), ('Futures', df_futures3), ('Revenue Summary', df_summary3)]:
                ws = writer.sheets[sheet_name]
                for idx, col in enumerate(df_out.columns):
                    max_len = max(df_out[col].astype(str).map(len).max(), len(col)) + 2
                    ws.set_column(idx, idx, max_len)

        st.download_button(
            label="Download Coins Buy Comparison Report",
            data=output3.getvalue(),
            file_name="coinsbuy_order_comparison.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("Please upload the Coins Buy file and at least one Order-list file for Coins Buy.")
