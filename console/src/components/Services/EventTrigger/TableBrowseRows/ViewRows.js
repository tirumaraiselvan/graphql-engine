import React from 'react';
import ReactTable from 'react-table';
import 'react-table/react-table.css';
import { deleteItem, vExpandRow, vCollapseRow } from './ViewActions'; // eslint-disable-line no-unused-vars
import FilterQuery from './FilterQuery';
import {
  setOrderCol,
  setOrderType,
  removeOrder,
  runQuery,
  setOffset,
  setLimit,
  addOrder,
} from './FilterActions';
import { ordinalColSort } from '../utils';
import Spinner from '../../../Common/Spinner/Spinner';
import './ReactTableFix.css';

const ViewRows = ({
  curTriggerName,
  curQuery,
  curFilter,
  curRows,
  curPath,
  curDepth,
  activePath,
  triggerList,
  dispatch,
  isProgressing,
  isView,
  count,
  expandedRow,
}) => {
  const styles = require('../TableCommon/Table.scss');
  const triggerSchema = triggerList.find(x => x.name === curTriggerName);
  const curRelName = curPath.length > 0 ? curPath.slice(-1)[0] : null;

  // Am I a single row display
  const isSingleRow = false;

  // Get the headings
  const tableHeadings = [];
  const gridHeadings = [];
  const eventLogColumns = [
    'id',
    'payload',
    'webhook',
    'delivered',
    'created_at',
  ];
  const sortedColumns = eventLogColumns.sort(ordinalColSort);

  if (!isView) {
    gridHeadings.push({
      Header: '',
      accessor: 'actions',
    });
  }

  sortedColumns.map((column, i) => {
    tableHeadings.push(<th key={i}>{column}</th>);
    gridHeadings.push({
      Header: column,
      accessor: column,
    });
  });

  tableHeadings.push(
    <th
      key="relIndicator"
      style={{ minWidth: 'auto', color: '#aaa', fontWeight: 300 }}
    >
      {' '}
      &lt;&gt;{' '}
    </th>
  );

  const hasPrimaryKeys = true;
  let editButton;
  let deleteButton;

  const newCurRows = [];
  if (curRows && curRows[0] && curRows[0].event_logs) {
    curRows[0].event_logs.forEach((row, rowIndex) => {
      const newRow = {};
      const pkClause = {};
      if (!isView && hasPrimaryKeys) {
        pkClause.id = row.id;
      } else {
        triggerSchema.map(k => {
          pkClause[k] = row[k];
        });
      }
      if (!isSingleRow && !isView && hasPrimaryKeys) {
        deleteButton = (
          <button
            className={`${styles.add_mar_right_small} btn btn-xs btn-default`}
            onClick={() => {
              dispatch(deleteItem(pkClause));
            }}
            data-test={`row-delete-button-${rowIndex}`}
          >
            Delete
          </button>
        );
      }
      const buttonsDiv = (
        <div className={styles.tableCellCenterAligned}>
          {editButton}
          {deleteButton}
        </div>
      );
      // Insert Edit, Delete, Clone in a cell
      newRow.actions = buttonsDiv;
      // Insert cells corresponding to all rows
      sortedColumns.forEach(col => {
        const getCellContent = () => {
          let conditionalClassname = styles.tableCellCenterAligned;
          const cellIndex = `${curTriggerName}-${col}-${rowIndex}`;
          if (expandedRow === cellIndex) {
            conditionalClassname = styles.tableCellCenterAlignedExpanded;
          }
          if (row[col] === null) {
            return (
              <div className={conditionalClassname}>
                <i>NULL</i>
              </div>
            );
          }
          let content = row[col] === undefined ? 'NULL' : row[col].toString();
          if (col === 'payload') {
            content = JSON.stringify(row[col]);
          }
          const expandOrCollapseBtn =
            expandedRow === cellIndex ? (
              <i
                className={`${styles.cellCollapse} fa fa-minus`}
                onClick={() => dispatch(vCollapseRow())}
              >
                {' '}
              </i>
            ) : (
              <i
                className={`${styles.cellExpand} fa fa-expand`}
                onClick={() => dispatch(vExpandRow(cellIndex))}
              >
                {' '}
              </i>
            );
          if (content.length > 20) {
            return (
              <div className={conditionalClassname}>
                {expandOrCollapseBtn}
                {content}
              </div>
            );
          }
          return <div className={conditionalClassname}>{content}</div>;
        };
        newRow[col] = getCellContent();
      });
      newCurRows.push(newRow);
    });
  }

  // Is this ViewRows visible
  let isVisible = false;
  if (!curRelName) {
    isVisible = true;
  } else if (curRelName === activePath[curDepth]) {
    isVisible = true;
  }

  let filterQuery = null;
  if (!isSingleRow) {
    if (curRelName === activePath[curDepth] || curDepth === 0) {
      // Rendering only if this is the activePath or this is the root

      let wheres = [{ '': { '': '' } }];
      if ('where' in curFilter && '$and' in curFilter.where) {
        wheres = [...curFilter.where.$and];
      }

      let orderBy = [{ column: '', type: 'asc', nulls: 'last' }];
      if ('order_by' in curFilter) {
        orderBy = [...curFilter.order_by];
      }
      const limit = 'limit' in curFilter ? curFilter.limit : 10;
      const offset = 'offset' in curFilter ? curFilter.offset : 0;

      filterQuery = (
        <FilterQuery
          curQuery={curQuery}
          whereAnd={wheres}
          triggerSchema={triggerSchema}
          orderBy={orderBy}
          limit={limit}
          dispatch={dispatch}
          count={count}
          triggerName={curTriggerName}
          offset={offset}
        />
      );
    }
  }

  const sortByColumn = col => {
    // Remove all the existing order_bys
    const numOfOrderBys = curFilter.order_by.length;
    for (let i = 0; i < numOfOrderBys - 1; i++) {
      dispatch(removeOrder(1));
    }
    // Go back to the first page
    dispatch(setOffset(0));
    // Set the filter and run query
    dispatch(setOrderCol(col, 0));
    if (
      curFilter.order_by.length !== 0 &&
      curFilter.order_by[0].column === col &&
      curFilter.order_by[0].type === 'asc'
    ) {
      dispatch(setOrderType('desc', 0));
    } else {
      dispatch(setOrderType('asc', 0));
    }
    dispatch(runQuery(triggerSchema));
    // Add a new empty filter
    dispatch(addOrder());
  };

  const changePage = page => {
    if (curFilter.offset !== page * curFilter.limit) {
      dispatch(setOffset(page * curFilter.limit));
      dispatch(runQuery(triggerSchema));
    }
  };

  const changePageSize = size => {
    if (curFilter.size !== size) {
      dispatch(setLimit(size));
      dispatch(runQuery(triggerSchema));
    }
  };

  const renderTableBody = () => {
    if (isProgressing) {
      return (
        <div>
          {' '}
          <Spinner />{' '}
        </div>
      );
    } else if (count === 0) {
      return <div> No rows found. </div>;
    }
    let shouldSortColumn = true;
    return (
      <ReactTable
        className="-highlight"
        data={newCurRows}
        columns={gridHeadings}
        resizable
        manual
        sortable={false}
        minRows={0}
        getTheadThProps={(finalState, some, column) => ({
          onClick: () => {
            if (
              column.Header &&
              shouldSortColumn &&
              column.Header !== 'Actions'
            ) {
              sortByColumn(column.Header);
            }
            shouldSortColumn = true;
          },
        })}
        getResizerProps={(finalState, none, column, ctx) => ({
          onMouseDown: e => {
            shouldSortColumn = false;
            ctx.resizeColumnStart(e, column, false);
          },
        })}
        showPagination={count > curFilter.limit}
        defaultPageSize={Math.min(curFilter.limit, count)}
        pages={Math.ceil(count / curFilter.limit)}
        onPageChange={changePage}
        onPageSizeChange={changePageSize}
        page={Math.floor(curFilter.offset / curFilter.limit)}
      />
    );
  };

  return (
    <div className={isVisible ? '' : 'hide '}>
      {filterQuery}
      <hr />
      <div className="row">
        <div className="col-xs-12">
          <div className={styles.tableContainer}>{renderTableBody()}</div>
          <br />
          <br />
        </div>
      </div>
    </div>
  );
};

export default ViewRows;
