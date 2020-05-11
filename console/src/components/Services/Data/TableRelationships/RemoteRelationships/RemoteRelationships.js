import React, { useEffect } from 'react';
import styles from '../../TableModify/ModifyTable.scss';
import { parseRemoteRelationship } from '../utils';
import { setRemoteRelationships, defaultRemoteRelationship } from '../Actions';
import RemoteRelationshipEditor from './RemoteRelationshipEditor';

const RemoteRelationships = ({
  remoteRelationships,
  dispatch,
  tableSchema,
  remoteSchemas,
  adminHeaders,
}) => {
  // restructure existing relationships to the state structure
  const existingRemoteRelationships = tableSchema.remote_relationships.map(
    ({ configuration, definition }) => {
      // remote schema found in definition !4485
      const config = configuration || definition || {};
      return parseRemoteRelationship({
        // todo check if needs to be defaulted with'' or not
        remote_schema: config?.remote_schema || '',
        remote_field: config?.remote_field || '',
        name: config?.name || '',
      });
    }
  );
  existingRemoteRelationships.push({
    ...defaultRemoteRelationship,
  });
  useEffect(() => {
    dispatch(setRemoteRelationships(existingRemoteRelationships));
  }, [tableSchema]);

  // render list of relationships
  const relationshipList = () => {
    const numRels = remoteRelationships.relationships.length;
    return remoteRelationships.relationships.map((remoteRelationship, i) => {
      return (
        <RemoteRelationshipEditor
          relationship={remoteRelationship}
          allRelationships={remoteRelationships.relationships}
          existingRelationship={existingRemoteRelationships[i]}
          index={i}
          numRels={numRels}
          dispatch={dispatch}
          tableSchema={tableSchema}
          remoteSchemas={remoteSchemas}
          key={
            existingRemoteRelationships[i]
              ? existingRemoteRelationships[i].name
              : 'new-remote-rel'
          }
          adminHeaders={adminHeaders}
        />
      );
    });
  };

  return (
    <div>
      <div className={styles.add_mar_bottom}>
        Relationships to remote schemas
      </div>
      <div>{relationshipList()}</div>
    </div>
  );
};

export default RemoteRelationships;
